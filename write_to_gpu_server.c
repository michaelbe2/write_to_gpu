/*
 * Copyright (c) 2005 Topspin Communications.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#if HAVE_CONFIG_H
#  include <config.h>
#endif /* HAVE_CONFIG_H */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netdb.h>
#include <malloc.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <time.h>

#include "write_to_gpu.h"

static int debug = 1;
static int debug_fast_path = 1;
#define DEBUG_LOG if (debug) printf
#define DEBUG_LOG_FAST_PATH if (debug_fast_path) printf
#define FDEBUG_LOG if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH if (debug_fast_path) fprintf

enum {
    PINGPONG_RECV_WRID = 1,
    PINGPONG_SEND_WRID = 2,
};

static int page_size;
static void *contig_addr;

struct rdma_cb {
    struct ibv_context      *context;
    struct ibv_comp_channel *channel;
    struct ibv_pd           *pd;
    struct ibv_mr           *mr;
    struct ibv_cq           *cq;
    struct ibv_qp           *qp;
    void                    *buf;
    unsigned long           size; // changed from "unsigned long long"
    int                     rx_depth;
    int                     pending;
    struct ibv_port_attr    portinfo;
    int                     sockfd;
    unsigned long long      gpu_buf_addr;
    unsigned long           gpu_buf_size;
    unsigned long           gpu_buf_rkey;
};

struct pingpong_dest {
    int lid;
    int qpn;
    int psn;
    union ibv_gid gid;
};

static int pp_connect_ctx(struct rdma_cb *cb, int port, int my_psn,
                          enum ibv_mtu mtu, struct pingpong_dest *dest, int sgid_idx)
{
    struct ibv_qp_attr attr = {
        .qp_state           = IBV_QPS_RTR,
        .path_mtu           = mtu,
        .dest_qp_num        = dest->qpn,
        .rq_psn             = dest->psn,
        .max_dest_rd_atomic = 1,
        .min_rnr_timer      = 12,
        .ah_attr            = {
            .is_global      = 0,
            .dlid           = dest->lid,
            .sl             = 0,
            .src_path_bits  = 0,
            .port_num       = port
        }
    };
    enum ibv_qp_attr_mask attr_mask;

    if (dest->gid.global.interface_id) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.hop_limit = 1;
        attr.ah_attr.grh.dgid = dest->gid;
        attr.ah_attr.grh.sgid_index = sgid_idx;
    }
    attr_mask = IBV_QP_STATE          |
            IBV_QP_AV                 |
            IBV_QP_PATH_MTU           |
            IBV_QP_DEST_QPN           |
            IBV_QP_RQ_PSN             |
            IBV_QP_MAX_DEST_RD_ATOMIC |
            IBV_QP_MIN_RNR_TIMER;

    if (ibv_modify_qp(cb->qp, &attr, attr_mask)) {
        fprintf(stderr, "Failed to modify QP to RTR\n");
        return 1;
    }

    attr.qp_state       = IBV_QPS_RTS;
    attr.timeout        = 14;
    attr.retry_cnt      = 7;
    attr.rnr_retry      = 7;
    attr.sq_psn         = my_psn;
    attr.max_rd_atomic  = 1;
    if (ibv_modify_qp(cb->qp, &attr,
                  IBV_QP_STATE              |
                  IBV_QP_TIMEOUT            |
                  IBV_QP_RETRY_CNT          |
                  IBV_QP_RNR_RETRY          |
                  IBV_QP_SQ_PSN             |
                  IBV_QP_MAX_QP_RD_ATOMIC)) {
        fprintf(stderr, "Failed to modify QP to RTS\n");
        return 1;
    }

    return 0;
}

static struct pingpong_dest *pp_server_exch_dest(struct rdma_cb *cb,
                         int ib_port, enum ibv_mtu mtu, int port,
                         const struct pingpong_dest *my_dest, int sgid_idx)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_flags    = AI_PASSIVE,
        .ai_family   = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int  rc, r_size;
    int  tmp_sockfd = -1;
    struct pingpong_dest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    rc = getaddrinfo(NULL, service, &hints, &res);

    if (rc < 0) {
        fprintf(stderr, "%s for port %d\n", gai_strerror(rc), port);
        free(service);
        return NULL;
    }

    for (t = res; t; t = t->ai_next) {
        tmp_sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (tmp_sockfd >= 0) {
            int optval = 1;

            setsockopt(tmp_sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval);

            if (!bind(tmp_sockfd, t->ai_addr, t->ai_addrlen))
                break;
            close(tmp_sockfd);
            tmp_sockfd = -1;
        }
    }

    freeaddrinfo(res);
    free(service);

    if (tmp_sockfd < 0) {
        fprintf(stderr, "Couldn't listen to port %d\n", port);
        return NULL;
    }

    listen(tmp_sockfd, 1);
    cb->sockfd = accept(tmp_sockfd, NULL, 0);
    close(tmp_sockfd);
    if (cb->sockfd < 0) {
        fprintf(stderr, "accept() failed\n");
        return NULL;
    } 

    r_size = recv(cb->sockfd, msg, sizeof(msg), MSG_WAITALL);
    if (r_size != sizeof msg) {
        perror("server read");
        fprintf(stderr, "%d/%d: Couldn't read remote address\n", r_size, (int) sizeof msg);
        return NULL;
    }
    DEBUG_LOG ("exch_dest: after receive \"%s\"\n", msg);

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest) {
        return NULL;
    }

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);
    DEBUG_LOG ("Rem GID: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
           rem_dest->gid.raw[0], rem_dest->gid.raw[1], rem_dest->gid.raw[2], rem_dest->gid.raw[3],
           rem_dest->gid.raw[4], rem_dest->gid.raw[5], rem_dest->gid.raw[6], rem_dest->gid.raw[7], 
           rem_dest->gid.raw[8], rem_dest->gid.raw[9], rem_dest->gid.raw[10], rem_dest->gid.raw[11],
           rem_dest->gid.raw[12], rem_dest->gid.raw[13], rem_dest->gid.raw[14], rem_dest->gid.raw[15] );

    if (pp_connect_ctx(cb, ib_port, my_dest->psn, mtu, rem_dest, sgid_idx)) {
        fprintf(stderr, "Couldn't connect to remote QP\n");
        free(rem_dest);
        return NULL;
    }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    DEBUG_LOG ("exch_dest:  before send  \"%s\"\n", msg);
    if (write(cb->sockfd, msg, sizeof msg) != sizeof msg) {
        fprintf(stderr, "Couldn't send local address\n");
        free(rem_dest);
        return NULL;
    }

    return rem_dest;
}

static struct rdma_cb *pp_init_ctx(struct ibv_device *ib_dev, unsigned long long size,
                                            int rx_depth, int port, int use_event)
{
    struct rdma_cb *cb;
    int ret;

    cb = calloc(1, sizeof *cb);
    if (!cb)
        return NULL;


    cb->size     = size;
    cb->rx_depth = rx_depth;
    cb->sockfd   = -1;

    cb->buf = memalign(page_size, size);
    if (!cb->buf) {
        fprintf(stderr, "Couldn't allocate work buf.\n");
        goto clean_ctx;
    }

    cb->context = ibv_open_device(ib_dev);
    if (!cb->context) {
        fprintf(stderr, "Couldn't get context for %s\n",
            ibv_get_device_name(ib_dev));
        goto clean_buffer;
    }

    if (use_event) {
        cb->channel = ibv_create_comp_channel(cb->context);
        if (!cb->channel) {
            fprintf(stderr, "Couldn't create completion channel\n");
            goto clean_device;
        }
    } else
        cb->channel = NULL;

    cb->pd = ibv_alloc_pd(cb->context);
    if (!cb->pd) {
        fprintf(stderr, "Couldn't allocate PD\n");
        goto clean_comp_channel;
    }

    cb->mr = ibv_reg_mr(cb->pd, cb->buf, size,
                         //IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ );
                         IBV_ACCESS_LOCAL_WRITE);

    if (!cb->mr) {
        fprintf(stderr, "Couldn't register MR\n");
        goto clean_pd;
    }
    
    /* FIXME memset(cb->buf, 0, size); */
    memset(cb->buf, 0x7b, size);

    DEBUG_LOG ("ibv_create_cq(%p, %d, NULL, %p, 0)\n", cb->context, rx_depth + 1, cb->channel);
    cb->cq = ibv_create_cq(cb->context, rx_depth + 1, NULL, cb->channel, 0);
    if (!cb->cq) {
        fprintf(stderr, "Couldn't create CQ\n");
        goto clean_mr;
    }

    {
        struct ibv_qp_init_attr attr = {
            .send_cq = cb->cq,
            .recv_cq = cb->cq,
            .cap     = {
                .max_send_wr  = 1,
                .max_recv_wr  = rx_depth,
                .max_send_sge = 1,
                .max_recv_sge = 1
            },
            .qp_type = IBV_QPT_RC,
        };

        DEBUG_LOG ("ibv_create_qp(%p,%p)\n", cb->pd, &attr);
        cb->qp = ibv_create_qp(cb->pd, &attr);

        if (!cb->qp)  {
            fprintf(stderr, "Couldn't create QP\n");
            goto clean_cq;
        }
    }

    {
        struct ibv_qp_attr attr = {
            .qp_state        = IBV_QPS_INIT,
            .pkey_index      = 0,
            .port_num        = port,
            //.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ
            .qp_access_flags = IBV_ACCESS_LOCAL_WRITE
        };

        if (ibv_modify_qp(cb->qp, &attr,
                  IBV_QP_STATE              |
                  IBV_QP_PKEY_INDEX         |
                  IBV_QP_PORT               |
                  IBV_QP_ACCESS_FLAGS)) {
            fprintf(stderr, "Failed to modify QP to INIT\n");
            goto clean_qp;
        }
    }

    return cb;

clean_qp:
    ibv_destroy_qp(cb->qp);

clean_cq:
    ibv_destroy_cq(cb->cq);

clean_mr:
    ibv_dereg_mr(cb->mr);

clean_pd:
    ibv_dealloc_pd(cb->pd);

clean_comp_channel:
    if (cb->channel)
        ibv_destroy_comp_channel(cb->channel);

clean_device:
    ibv_close_device(cb->context);

clean_buffer:
    free(cb->buf);

clean_ctx:
    free(cb);

    return NULL;
}

int pp_close_ctx(struct rdma_cb *cb)
{
    if (ibv_destroy_qp(cb->qp)) {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

    if (ibv_destroy_cq(cb->cq)) {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }

    if (ibv_dereg_mr(cb->mr)) {
        fprintf(stderr, "Couldn't deregister MR\n");
        return 1;
    }

    if (ibv_dealloc_pd(cb->pd)) {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (cb->channel) {
        if (ibv_destroy_comp_channel(cb->channel)) {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }

    if (ibv_close_device(cb->context)) {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }

    free(cb->buf);

    if (cb->sockfd != -1)
        close(cb->sockfd);
    
    free(cb);

    return 0;
}

#define mmin(a, b) a < b ? a : b

static int pp_post_recv(struct rdma_cb *cb, int n) //
{                                                            //
    struct ibv_sge list = {                                  //
        .addr   = (uintptr_t) cb->buf,                      //
        .length = cb->size,                                 //
        .lkey   = cb->mr->lkey                              //
    };                                                       //
    struct ibv_recv_wr wr = {                                //
        .wr_id      = PINGPONG_RECV_WRID,                    //
        .sg_list    = &list,                                 //
        .num_sge    = 1,
        .next       = NULL                                     //
    };                                                       //
    struct ibv_recv_wr *bad_wr;                              //
    int i;                                                   //
                                                             //
    for (i = 0; i < n; ++i)                                  //
        if (ibv_post_recv(cb->qp, &wr, &bad_wr))            //
            break;                                           //
                                                             //
    return i;                                                //
}                                                            //

static int pp_post_send(struct rdma_cb *cb)
{
    struct ibv_sge list = {
        .addr   = (uintptr_t) cb->buf,
        .length = cb->size,
        .lkey   = cb->mr->lkey
    };
    struct ibv_send_wr wr = {
        .wr_id      = PINGPONG_SEND_WRID,
        .sg_list    = &list,
        .num_sge    = 1,
        .opcode     = IBV_WR_RDMA_WRITE, //IBV_WR_SEND
        .send_flags = IBV_SEND_SIGNALED,
        .wr.rdma.remote_addr = cb->gpu_buf_addr,
        .wr.rdma.rkey        = cb->gpu_buf_rkey,
        .next       = NULL
    };
    struct ibv_send_wr *bad_wr;

    DEBUG_LOG_FAST_PATH("ibv_post_send IBV_WR_RDMA_WRITE: local buf 0x%llx, lkey 0x%x, remote buf 0x%llx, rkey = 0x%x, size = %u\n",
                        (unsigned long long)wr.sg_list[0].addr, wr.sg_list[0].lkey,
                        (unsigned long long)wr.wr.rdma.remote_addr, wr.wr.rdma.rkey, wr.sg_list[0].length);
    return ibv_post_send(cb->qp, &wr, &bad_wr);
}

static void usage(const char *argv0)
{
    printf("Usage:\n");
    printf("  %s            start a server and wait for connection\n", argv0);
    printf("\n");
    printf("Options:\n");
    printf("  -p, --port=<port>         listen on/connect to port <port> (default 18515)\n");
    printf("  -d, --ib-dev=<dev>        use IB device <dev> (default first device found)\n");
    printf("  -i, --ib-port=<port>      use port <port> of IB device (default 1)\n");
    printf("  -s, --size=<size>         size of message to exchange (default 4096)\n");
    printf("  -m, --mtu=<size>          path MTU (default 1024)\n");
    printf("  -r, --rx-depth=<dep>      number of receives to post at a time (default 500)\n");
    printf("  -n, --iters=<iters>       number of exchanges (default 1000)\n");
    printf("  -e, --events              sleep on CQ events (default poll)\n");
    printf("  -g, --gid-idx=<gid index> local port gid index\n");
}

int main(int argc, char *argv[])
{
    struct ibv_device      **dev_list;
    struct ibv_device   *ib_dev;
    struct rdma_cb *cb;
    struct pingpong_dest     my_dest;
    struct pingpong_dest    *rem_dest;
    struct timeval           start, end;
    char                    *ib_devname = NULL;
    int                      port = 18515;
    int                      ib_port = 1;
    unsigned long long       size = 4096;
    enum ibv_mtu             mtu = IBV_MTU_1024;
    int                      rx_depth = 500;
    int                      iters = 1000;
    int                      use_event = 0;
    int                      routs;
    int                      scnt;
    int                      num_cq_events = 0;
    int                      gidx = -1;
    char                     gid[INET6_ADDRSTRLEN];
    int                      inlr_recv = 0;
    int                      err;

    srand48(getpid() * time(NULL));
    contig_addr = NULL;

    while (1) {
        int c;

        static struct option long_options[] = {
            { .name = "port",          .has_arg = 1, .val = 'p' },
            { .name = "ib-dev",        .has_arg = 1, .val = 'd' },
            { .name = "ib-port",       .has_arg = 1, .val = 'i' },
            { .name = "size",          .has_arg = 1, .val = 's' },
            { .name = "mtu",           .has_arg = 1, .val = 'm' },
            { .name = "rx-depth",      .has_arg = 1, .val = 'r' },
            { .name = "iters",         .has_arg = 1, .val = 'n' },
            { .name = "events",        .has_arg = 0, .val = 'e' },
            { .name = "gid-idx",       .has_arg = 1, .val = 'g' },
            { 0 }
        };

        c = getopt_long(argc, argv, "p:d:i:s:m:r:n:eg:",
                long_options, NULL);
        if (c == -1)
            break;

        switch (c) {
        
        case 'p':
            port = strtol(optarg, NULL, 0);
            if (port < 0 || port > 65535) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 'd':
            ib_devname = strdupa(optarg);
            break;

        case 'i':
            ib_port = strtol(optarg, NULL, 0);
            if (ib_port < 0) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 's':
            size = strtoll(optarg, NULL, 0);
            break;

        case 'm':
            mtu = pp_mtu_to_enum(strtol(optarg, NULL, 0));
            if (mtu < 0) {
                usage(argv[0]);
                return 1;
            }
            break;

        case 'r':
            rx_depth = strtol(optarg, NULL, 0);
            break;

        case 'n':
            iters = strtol(optarg, NULL, 0);
            break;

        case 'e':
            ++use_event;
            break;

        case 'g':
            gidx = strtol(optarg, NULL, 0);
            break;

        default:
            usage(argv[0]);
            return 1;
        }
    }

    if (optind < argc) {
        usage(argv[0]);
        return 1;
    }

    page_size = sysconf(_SC_PAGESIZE);

    /****************************************************************************************************
     * In the next block we are checking if given IB device name matches one of devices in the list.
     * If the name is not given, we take the first available IB device, else the matching to the name one.
     * The result of this block is ig_dev - initialized pointer to the relevant struct ibv_device
     */
    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    if (!ib_devname) /*if ib device name is not given by comand line*/{
        DEBUG_LOG ("Device name is not given by command line, taking the first available from device list\n");
        ib_dev = *dev_list;
        if (!ib_dev) {
            fprintf(stderr, "No IB devices found\n");
            return 1;
        }
    } else {
        int i;
        for (i = 0; dev_list[i]; ++i)
            if (!strcmp(ibv_get_device_name(dev_list[i]), ib_devname))
                break;
        ib_dev = dev_list[i];
        if (!ib_dev) {
            fprintf(stderr, "IB device %s not found\n", ib_devname);
            return 1;
        }
    }
    /****************************************************************************************************/

    cb = pp_init_ctx(ib_dev, size, rx_depth, ib_port, use_event);
    if (!cb)
        return 1;

    //routs = pp_post_recv(cb, cb->rx_depth);                   
    //if (routs < cb->rx_depth) {                                
    //    fprintf(stderr, "Couldn't post receive (%d)\n", routs); 
    //    return 1;                                               
    //}                                                           

    if (use_event)
        if (ibv_req_notify_cq(cb->cq, 0)) {
            fprintf(stderr, "Couldn't request CQ notification\n");
            return 1;
        }


    if (pp_get_port_info(cb->context, ib_port, &cb->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = cb->portinfo.lid;
    if (cb->portinfo.link_layer != IBV_LINK_LAYER_ETHERNET &&
                            !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    if (gidx >= 0) {
        if (ibv_query_gid(cb->context, ib_port, gidx, &my_dest.gid)) {
            fprintf(stderr, "can't read sgid of index %d\n", gidx);
            return 1;
        }
        DEBUG_LOG ("My GID: %02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x\n",
               my_dest.gid.raw[0], my_dest.gid.raw[1], my_dest.gid.raw[2], my_dest.gid.raw[3],
               my_dest.gid.raw[4], my_dest.gid.raw[5], my_dest.gid.raw[6], my_dest.gid.raw[7], 
               my_dest.gid.raw[8], my_dest.gid.raw[9], my_dest.gid.raw[10], my_dest.gid.raw[11],
               my_dest.gid.raw[12], my_dest.gid.raw[13], my_dest.gid.raw[14], my_dest.gid.raw[15] );
    } else
        memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = cb->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);
    printf("  local address:  LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           my_dest.lid, my_dest.qpn, my_dest.psn, gid);


    rem_dest = pp_server_exch_dest(cb, ib_port, mtu, port, &my_dest, gidx);

    if (!rem_dest) {
        pp_close_ctx(cb);
        return 1;
    }

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);
    printf("  remote address: LID 0x%04x, QPN 0x%06x, PSN 0x%06x, GID %s\n",
           rem_dest->lid, rem_dest->qpn, rem_dest->psn, gid);

    cb->pending = PINGPONG_RECV_WRID;

    if (gettimeofday(&start, NULL)) {
        perror("gettimeofday");
        return 1;
    }

    scnt = 0;
    /****************************************************************************************************
     * The main loop where we client and server send and receive "iters" number of messages
     */
    //for (cnt = 0; cnt < iters; cnt++) {
    while (scnt < iters) {

        int  r_size;
        char receivemsg[sizeof "0102030405060708:01020304:01020304"];
        char ackmsg[sizeof "rdma_write completed"];

        // Receiving RDMA data (address and rkey) from socket as a triger to start RDMA write operation
        DEBUG_LOG_FAST_PATH("Iteration %d: Waiting to Receive message\n", scnt);
        r_size = recv(cb->sockfd, receivemsg, sizeof(receivemsg), MSG_WAITALL);
        if (r_size != sizeof receivemsg) {
            fprintf(stderr, "Couldn't receive RDMA data for iteration %d\n", scnt);
            return 1;
        }
        sscanf(receivemsg, "%llx:%lx:%lx", &cb->gpu_buf_addr, &cb->gpu_buf_size, &cb->gpu_buf_rkey);
        DEBUG_LOG_FAST_PATH("The message received: \"%s\", gpu_buf_addr = 0x%llx, gpu_buf_size = %lu, gpu_buf_rkey = 0x%lx\n",
                            receivemsg, (unsigned long long)cb->gpu_buf_addr, cb->gpu_buf_size, cb->gpu_buf_rkey);
        cb->size = mmin(cb->size, cb->gpu_buf_size);
        
        // Executing RDMA write
        sprintf((char*)cb->buf, "Write iteration N %d", scnt);
        if (pp_post_send(cb)) {
            fprintf(stderr, "Couldn't post send\n");
            return 1;
        }
        // Waiting for completion event
        if (use_event) {
            struct ibv_cq *ev_cq;
            void          *ev_ctx;

            DEBUG_LOG_FAST_PATH("Waiting for completion event\n");
            if (ibv_get_cq_event(cb->channel, &ev_cq, &ev_ctx)) {
                fprintf(stderr, "Failed to get cq_event\n");
                return 1;
            }

            ++num_cq_events;

            if (ev_cq != cb->cq) {
                fprintf(stderr, "CQ event for unknown CQ %p\n", ev_cq);
                return 1;
            }

            if (ibv_req_notify_cq(cb->cq, 0)) {
                fprintf(stderr, "Couldn't request CQ notification\n");
                return 1;
            }
        }
        // Polling completion queue
        DEBUG_LOG_FAST_PATH("Polling completion queue\n");
        {
            struct ibv_wc wc[2];
            int ne, i;

            do {
                DEBUG_LOG_FAST_PATH("Before ibv_poll_cq\n");
                ne = ibv_poll_cq(cb->cq, 2, wc);
                if (ne < 0) {
                    fprintf(stderr, "poll CQ failed %d\n", ne);
                    return 1;
                }
            } while (!use_event && ne < 1);

            for (i = 0; i < ne; ++i) {
                if (wc[i].status != IBV_WC_SUCCESS) {
                    fprintf(stderr, "Failed status \"%s\" (%d) for wr_id %d\n",
                            ibv_wc_status_str(wc[i].status),
                            wc[i].status, (int) wc[i].wr_id);
                    return 1;
                }

                switch ((int) wc[i].wr_id) {
                case PINGPONG_SEND_WRID:
                    ++scnt;
                    break;
                default:
                    fprintf(stderr, "Completion for unknown wr_id %d\n", (int) wc[i].wr_id);
                    return 1;
                }
            }
        }
        // Sending ack-message to the client, confirming that RDMA write has been completet
        if (write(cb->sockfd, "rdma_write completed", sizeof("rdma_write completed")) != sizeof("rdma_write completed")) {
            fprintf(stderr, "Couldn't send \"rdma_write completed\" msg\n");
            return 1;
        }
    }
    /****************************************************************************************************/

    if (gettimeofday(&end, NULL)) {
        perror("gettimeofday");
        return 1;
    }

    {
        float usec = (end.tv_sec - start.tv_sec) * 1000000 +
            (end.tv_usec - start.tv_usec);
        long long bytes = (long long) size * iters * 2;

        printf("%lld bytes in %.2f seconds = %.2f Mbit/sec\n",
               bytes, usec / 1000000., bytes * 8. / usec);
        printf("%d iters in %.2f seconds = %.2f usec/iter\n",
               iters, usec / 1000000., usec / iters);
    }

    ibv_ack_cq_events(cb->cq, num_cq_events);

    if (pp_close_ctx(cb))
        return 1;

    ibv_free_device_list(dev_list);
    free(rem_dest);

    return 0;
}
