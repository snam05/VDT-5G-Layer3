#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <netinet/tcp.h>

#define PORT_TCP 6000
#define HOST "127.0.0.1"

#define NGAP_INITIAL_UE_MSG 1
#define NGAP_REG_ACCEPT 2
#define NGAP_UE_CONTEXT_REL 3
#define NGAP_PAGING 100

#pragma pack(push, 1)
typedef struct
{
    uint32_t message_type;
    uint64_t suci;
    uint32_t ue_id; // 5G-S-TMSI
} ControlMsg;

typedef struct
{
    uint32_t message_type;
    uint32_t ue_id;
    uint32_t tac;
    uint32_t cn_domain;
} PagingMsg;
#pragma pack(pop)

std::vector<uint16_t> amf_database;
uint16_t tmsi_allocator = 10;
std::atomic<int> amf_msg_count(0);
std::atomic<int> stat_initial_rx(0);
std::atomic<int> stat_paging_tx(0);
std::atomic<int> stat_release_tx(0);

void monitor_thread()
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(5));
        int total = amf_msg_count.exchange(0);
        int initial = stat_initial_rx.exchange(0);
        int paging = stat_paging_tx.exchange(0);
        int release = stat_release_tx.exchange(0);

        if (total > 0)
        {
            printf("\n[AMF STATS] (5s) Tổng tin xử lý: %d | NgAP Rx Initial: %d | NgAP Tx Release: %d | NgAP Tx Paging: %d\n",
                   total, initial, release, paging);
        }
    }
}

void handle_ue_lifecycle(int sock_fd, uint16_t allocated_tmsi)
{
    // Chờ 2s để mô phỏng thời gian sử dụng dịch vụ trước khi về IDLE
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Gửi NGAP_UE_CONTEXT_REL
    ControlMsg rel_msg = {NGAP_UE_CONTEXT_REL, 0, allocated_tmsi};
        send(sock_fd, &rel_msg, sizeof(rel_msg), 0);
    stat_release_tx++;
    amf_msg_count++;

    // Đợi 3s trong trạng thái IDLE rồi có cuộc gọi tới -> PAGING
    std::this_thread::sleep_for(std::chrono::seconds(3));

    PagingMsg paging_msg = {NGAP_PAGING, allocated_tmsi, 100, 101};
        send(sock_fd, &paging_msg, sizeof(paging_msg), 0);
    stat_paging_tx++;
    amf_msg_count++;
}

int main()
{
    setbuf(stdout, NULL);
    std::thread(monitor_thread).detach();
    printf("[AMF] Khởi động AMF. Sẵn sàng xác thực và cấp Căn cước (5G-S-TMSI) cho thiết bị...\n");

    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT_TCP);
    inet_pton(AF_INET, HOST, &server_addr.sin_addr);

    while (connect(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0)
    {
        sleep(1);
    }
    // [C9] Tắt Nagle Algorithm: message 16-byte nhỏ sẽ không bị gom lại → giảm latency
    int flag = 1;
    setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
    printf("[AMF] Đã liên kết TCP với gNodeB.\n");

    ControlMsg msg;
    // [C4] MSG_WAITALL: block cho đến khi nhận đủ 16 byte, tránh partial read khi throughput cao
    while (recv(sock_fd, &msg, sizeof(msg), MSG_WAITALL) == (ssize_t)sizeof(msg))
    {
        amf_msg_count++;
        if (msg.message_type == NGAP_INITIAL_UE_MSG)
        {
            stat_initial_rx++;
            uint16_t allocated_tmsi = tmsi_allocator++;
            amf_database.push_back(allocated_tmsi);

            ControlMsg accept_msg = {NGAP_REG_ACCEPT, msg.suci, allocated_tmsi};
                send(sock_fd, &accept_msg, sizeof(accept_msg), 0);

            // Bật 1 luồng riêng biệt để đếm giờ cho UE này
            std::thread(handle_ue_lifecycle, sock_fd, allocated_tmsi).detach();
        }
    }
    close(sock_fd);
    return 0;
}
