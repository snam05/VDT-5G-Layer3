#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <chrono>
#include <thread>

#define PORT_UDP    5000
#define HOST        "127.0.0.1"
#define MIB_CYCLE   8       // Phát SSB sau mỗi 8 tick = 80 ms
#define CELL_ID     205

#define MAX_UES 100

using Clock    = std::chrono::steady_clock;
using Ms       = std::chrono::milliseconds;
using TimePoint = Clock::time_point;

static constexpr Ms TICK_MS{10};   // Chu kỳ mỗi tick = 10 ms

#pragma pack(push, 1)
typedef struct {
    uint8_t message_id;   // IE_1: cố định 0x01
    uint8_t sfn_msb6;     // IE_2: 6 bit cao của SFN (bits 9..4)
} MibContent;

typedef struct {
    int pss;
    int sss;
    MibContent mib;       // Bản tin MIB (chứa 6 bit cao SFN)
    uint8_t    sfn_lsb4;  // 4 bit thấp của SFN (bits 3..0)
} SsbPacket;
#pragma pack(pop)

typedef struct {
    int count;
    struct sockaddr_in addresses[MAX_UES];
} SharedData;

int main() {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_fd < 0) {
        perror("[gNodeB] Tạo socket thất bại");
        return EXIT_FAILURE;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT_UDP);
    inet_pton(AF_INET, HOST, &server_addr.sin_addr);  

    if (bind(sock_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("[gNodeB] Bind socket thất bại");
        close(sock_fd);
        return EXIT_FAILURE;
    }

    // Khởi tạo vùng nhớ chia sẻ (Shared Memory) để 2 tiến trình có thể giao tiếp
    SharedData *shared_data = (SharedData *)mmap(NULL, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (shared_data == MAP_FAILED) {
        perror("mmap thất bại");
        close(sock_fd);
        return EXIT_FAILURE;
    }
    shared_data->count = 0;

    pid_t pid = fork();

    if (pid < 0) {
        perror("Fork thất bại");
        close(sock_fd);
        return EXIT_FAILURE;
    }

    if (pid == 0) {
        // -------------------------------------------------------------------
        // TIẾN TRÌNH CON: Chỉ chịu trách nhiệm LẮNG NGHE (Listen)
        // -------------------------------------------------------------------
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        uint8_t signal;

        printf("[gNodeB - Lắng nghe] Luồng lắng nghe bắt đầu hoạt động...\n");

        while (1) {
            // Hàm recvfrom ở đây là BLOCKING, sẽ ngủ cho đến khi có dữ liệu đến (rất tối ưu CPU)
            if (recvfrom(sock_fd, &signal, sizeof(signal), 0, (struct sockaddr*)&client_addr, &client_len) > 0) {
                if (signal == 0x02) { // 0x02: Giả lập hành động mở ăng-ten dò sóng (Cell Search)
                    bool exists = false;
                    for (int i = 0; i < shared_data->count; i++) {
                        if (shared_data->addresses[i].sin_port == client_addr.sin_port && 
                            shared_data->addresses[i].sin_addr.s_addr == client_addr.sin_addr.s_addr) {
                            exists = true; 
                            break;
                        }
                    }
                    if (!exists && shared_data->count < MAX_UES) {
                        shared_data->addresses[shared_data->count] = client_addr;
                        shared_data->count++;
                        printf("[gNodeB - Lắng nghe] Có UE vừa dò thấy sóng (Tune In) từ port %d\n", ntohs(client_addr.sin_port));
                    }
                }
            }
        }
    } else {
        // -------------------------------------------------------------------
        // TIẾN TRÌNH CHA: Chỉ chịu trách nhiệm PHÁT SÓNG (Broadcast SSB)
        // -------------------------------------------------------------------
        uint16_t  gNodeB_sfn = 0;
        int       tick_count  = MIB_CYCLE;
        TimePoint next_tick   = Clock::now() + TICK_MS;
        printf("[gNodeB - Phát sóng] Khởi chạy thành công. Đang phát SSB mỗi %ld ms...\n", MIB_CYCLE * TICK_MS.count());

        while (1) {
            std::this_thread::sleep_until(next_tick);
            next_tick += TICK_MS;                       // lên lịch tick kế tiếp

            uint8_t sfn_lsb4 = gNodeB_sfn & 0x0F;       // 4 bit thấp của SFN

            // Đủ 8 tick (= 80 ms) → đóng gói và gửi SSB
            if (tick_count >= MIB_CYCLE) {
                SsbPacket ssb;
                ssb.pss = CELL_ID % 3;
                ssb.sss = CELL_ID / 3;
                ssb.mib.message_id = 0x01;
                ssb.mib.sfn_msb6   = (uint8_t)(gNodeB_sfn >> 4) & 0x3F;
                ssb.sfn_lsb4 = sfn_lsb4;

                ssize_t sent = 0;
                int current_count = shared_data->count; // Đọc từ shared memory
                
                for (int i = 0; i < current_count; i++) {
                    sent = sendto(sock_fd, &ssb, sizeof(ssb), 0,
                                  (struct sockaddr *)&shared_data->addresses[i], sizeof(struct sockaddr_in));
                }

                if (current_count > 0) {
                    if (sent < 0) {
                        perror("[gNodeB - Phát sóng] sendto thất bại");
                    } else {
                        printf("[gNodeB - Phát sóng] Đã gửi SSB cho %d UE -> SFN = %4d | MIB.sfn_msb6 = 0x%02X (%d) | sfn_lsb4 = 0x%X (%d)\n",
                               current_count,
                               gNodeB_sfn,
                               ssb.mib.sfn_msb6, ssb.mib.sfn_msb6,
                               ssb.sfn_lsb4,     ssb.sfn_lsb4);
                    }
                }

                tick_count = 0;
            }

            gNodeB_sfn = (gNodeB_sfn + 1) % 1024;
            tick_count++;
        }
    }

    close(sock_fd);
    munmap(shared_data, sizeof(SharedData));
    return 0;
}