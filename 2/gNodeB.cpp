#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <chrono>
#include <thread>
#include <vector>
#include <map>
#include <mutex>
#include <atomic>

#define PORT_UDP 5000
#define PORT_TCP 6000
#define HOST "127.0.0.1"
#define MIB_CYCLE 8

#define NGAP_INITIAL_UE_MSG 1
#define NGAP_REG_ACCEPT 2
#define NGAP_UE_CONTEXT_REL 3
#define NGAP_PAGING 100

#define RRC_REG_REQUEST 3
#define RRC_REG_ACCEPT 4
#define RRC_RELEASE 5
#define RRC_PAGING 100

using Clock = std::chrono::steady_clock;
using Ms = std::chrono::milliseconds;
using TimePoint = Clock::time_point;
static constexpr Ms TICK_MS{10};

#pragma pack(push, 1)
typedef struct
{
    uint8_t message_id;
    uint8_t sfn_msb6;
} MibContent;

typedef struct
{
    int pss;
    int sss;
    MibContent mib;
    uint8_t sfn_lsb4;
} SsbPacket;

typedef struct
{
    uint32_t message_type;
    uint64_t suci;
    uint32_t ue_id;
} ControlMsg;

typedef struct
{
    uint32_t message_type;
    uint32_t ue_id;
    uint32_t tac;
    uint32_t cn_domain;
} PagingMsg;
// Removed PagingGroupMsg
#pragma pack(pop)

// Quản lý môi trường vô tuyến chung
std::vector<struct sockaddr_in> air_interfaces;
std::mutex air_mutex;

// Quản lý trạng thái và Context của thiết bị đang kết nối
std::map<uint64_t, struct sockaddr_in> context_by_suci;
std::map<uint32_t, struct sockaddr_in> context_by_tmsi;
std::mutex ctx_mutex;

std::vector<PagingMsg> paging_queue;
std::mutex queue_mutex;

int sock_udp;
std::atomic<int> global_amf_fd(-1);
std::atomic<int> gnb_msg_count(0);
std::atomic<int> stat_rrc_req_rx(0);
std::atomic<int> stat_ngap_accept_rx(0);
std::atomic<int> stat_ngap_paging_rx(0);
std::atomic<int> stat_rrc_paging_tx(0);

void monitor_thread()
{
    while (1)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        int total = gnb_msg_count.exchange(0);
        int req_rx = stat_rrc_req_rx.exchange(0);
        int acc_rx = stat_ngap_accept_rx.exchange(0);
        int page_rx = stat_ngap_paging_rx.exchange(0);
        int page_tx = stat_rrc_paging_tx.exchange(0);

        if (total > 0)
        {
            printf("\n[gNodeB STATS] (5s) Tổng tin xử lý: %d | RRC Rx Req: %d | NgAP Rx Accept: %d | NgAP Rx Paging: %d | RRC Tx Paging: %d\n",
                   total, req_rx, acc_rx, page_rx, page_tx);
        }
    }
}

void tcp_listener_thread()
{
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT_TCP);
    bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr));
    listen(server_fd, 5);

    printf("[gNodeB] Đang chờ kết nối từ Core Network (TCP %d)...\n", PORT_TCP);

    while (1)
    {
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd >= 0)
        {
            global_amf_fd = client_fd;
            char buffer[16];
            while (recv(client_fd, buffer, 16, MSG_WAITALL) == 16)
            {
                uint32_t msg_type = *(uint32_t *)buffer;
                if (msg_type == NGAP_REG_ACCEPT)
                {
                    gnb_msg_count++;
                    stat_ngap_accept_rx++;
                    ControlMsg *msg = (ControlMsg *)buffer;
                    std::lock_guard<std::mutex> lock(ctx_mutex);
                    if (context_by_suci.count(msg->suci))
                    {
                        struct sockaddr_in ue_addr = context_by_suci[msg->suci];
                        context_by_tmsi[msg->ue_id] = ue_addr;

                        ControlMsg rrc_msg = {RRC_REG_ACCEPT, msg->suci, msg->ue_id};
                        sendto(sock_udp, &rrc_msg, sizeof(rrc_msg), 0, (struct sockaddr *)&ue_addr, sizeof(ue_addr));
                    }
                }
                else if (msg_type == NGAP_UE_CONTEXT_REL)
                {
                    ControlMsg *msg = (ControlMsg *)buffer;
                    std::lock_guard<std::mutex> lock(ctx_mutex);
                    if (context_by_tmsi.count(msg->ue_id))
                    {
                        struct sockaddr_in ue_addr = context_by_tmsi[msg->ue_id];

                        // Truyền tín hiệu giải phóng qua vô tuyến
                        ControlMsg rrc_msg = {RRC_RELEASE, 0, msg->ue_id};
                        sendto(sock_udp, &rrc_msg, sizeof(rrc_msg), 0, (struct sockaddr *)&ue_addr, sizeof(ue_addr));

                        // Xóa context (IDLE)
                        context_by_tmsi.erase(msg->ue_id);
                    }
                }
                else if (msg_type == NGAP_PAGING)
                {
                    gnb_msg_count++;
                    stat_ngap_paging_rx++;
                    PagingMsg *msg = (PagingMsg *)buffer;
                    std::lock_guard<std::mutex> lock(queue_mutex);
                    paging_queue.push_back(*msg);
                }
            }
            close(client_fd);
            global_amf_fd = -1;
        }
    }
}

void udp_listener_thread()
{
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    uint8_t buffer[256];

    while (1)
    {
        int bytes = recvfrom(sock_udp, buffer, sizeof(buffer), 0, (struct sockaddr *)&client_addr, &client_len);
        if (bytes > 0)
        {
            if (bytes == 1 && buffer[0] == 0x02)
            {
                std::lock_guard<std::mutex> lock(air_mutex);
                bool exists = false;
                for (auto &addr : air_interfaces)
                {
                    if (addr.sin_port == client_addr.sin_port)
                    {
                        exists = true;
                        break;
                    }
                }
                if (!exists)
                    air_interfaces.push_back(client_addr);
            }
            else if (bytes >= sizeof(uint32_t))
            {
                uint32_t msg_type = *(uint32_t *)buffer;
                if (msg_type == RRC_REG_REQUEST)
                {
                    gnb_msg_count++;
                    stat_rrc_req_rx++;
                    ControlMsg *req = (ControlMsg *)buffer;

                    {
                        std::lock_guard<std::mutex> lock(ctx_mutex);
                        context_by_suci[req->suci] = client_addr;
                    }

                    int amf_fd = global_amf_fd.load();
                    if (amf_fd >= 0)
                    {
                        ControlMsg ngap_req = {NGAP_INITIAL_UE_MSG, req->suci, 0};
                        send(amf_fd, &ngap_req, sizeof(ngap_req), 0);
                    }
                }
            }
        }
    }
}

int main()
{
    setbuf(stdout, NULL);
    sock_udp = socket(AF_INET, SOCK_DGRAM, 0);
    int sndbuf = 1024 * 1024 * 10; // 10MB UDP Send Buffer
    setsockopt(sock_udp, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT_UDP);
    inet_pton(AF_INET, HOST, &server_addr.sin_addr);
    bind(sock_udp, (struct sockaddr *)&server_addr, sizeof(server_addr));

    std::thread t_tcp(tcp_listener_thread);
    std::thread t_udp(udp_listener_thread);
    std::thread t_mon(monitor_thread);
    t_tcp.detach();
    t_udp.detach();
    t_mon.detach();

    uint16_t gNodeB_sfn = 0;
    int tick_count = MIB_CYCLE;
    TimePoint next_tick = Clock::now() + TICK_MS;

    printf("[gNodeB] Trạm phát khởi động thành công.\n");

    while (1)
    {
        std::this_thread::sleep_until(next_tick);
        next_tick += TICK_MS;

        std::vector<PagingMsg> local_queue;
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if (gNodeB_sfn % 64 == 0 && !paging_queue.empty())
            {
                local_queue = std::move(paging_queue);
                paging_queue.clear();
            }
        }

        if (!local_queue.empty())
        {
            for (size_t i = 0; i < local_queue.size(); i++)
            {
                PagingMsg p_msg;
                p_msg.message_type = RRC_PAGING;
                p_msg.ue_id = local_queue[i].ue_id;
                p_msg.tac = local_queue[i].tac;
                p_msg.cn_domain = local_queue[i].cn_domain;

                {
                    std::lock_guard<std::mutex> u_lock(air_mutex);
                    for (auto &addr : air_interfaces)
                    {
                        sendto(sock_udp, &p_msg, sizeof(p_msg), 0, (struct sockaddr *)&addr, sizeof(addr));
                    }
                }
                gnb_msg_count++;
                stat_rrc_paging_tx++;
                usleep(20); // Tránh Burst quá mạnh gây tràn hàng đợi mạng của OS
            }
        }

        if (tick_count >= MIB_CYCLE)
        {
            SsbPacket ssb;
            ssb.pss = 1;
            ssb.sss = 0;
            ssb.mib.message_id = 0x01;
            ssb.mib.sfn_msb6 = (uint8_t)(gNodeB_sfn >> 4) & 0x3F;
            ssb.sfn_lsb4 = gNodeB_sfn & 0x0F;

            std::lock_guard<std::mutex> u_lock(air_mutex);
            for (auto &addr : air_interfaces)
            {
                sendto(sock_udp, &ssb, sizeof(ssb), 0, (struct sockaddr *)&addr, sizeof(addr));
            }
            tick_count = 0;
        }

        gNodeB_sfn = (gNodeB_sfn + 1) % 1024;
        tick_count++;
    }
    return 0;
}
