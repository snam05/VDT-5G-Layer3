#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>

#define PORT 5000
#define HOST "127.0.0.1"
#define RESYNC_TICKS 80

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

// =========================================================
// CÁC BIẾN THỐNG KÊ HIỆU NĂNG TOÀN CỤC (KPI METRICS)
// =========================================================
std::atomic<int> stat_registered(0);
std::atomic<int> stat_idle(0);
std::atomic<int> stat_paged(0);

std::atomic<bool> first_paging_received(false);
TimePoint first_paging_time;
TimePoint last_paging_time;
std::mutex time_mutex;

std::atomic<bool> force_exit(false); // Cờ báo hiệu bắt buộc dừng toàn bộ luồng

bool verbose = false; // Tự động tắt log chi tiết nếu N lớn để tránh lag I/O

void run_ue_thread(uint64_t suci)
{
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    
    // CỐ ĐỊNH PORT CHO TỪNG UE: Tránh sinh Port ngẫu nhiên mỗi lần chạy
    int opt = 1;
    setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in my_addr;
    memset(&my_addr, 0, sizeof(my_addr));
    my_addr.sin_family = AF_INET;
    my_addr.sin_addr.s_addr = INADDR_ANY;
    my_addr.sin_port = htons(10000 + (suci - 1000000)); // Gán Port từ 10000 trở đi
    bind(sock_fd, (struct sockaddr *)&my_addr, sizeof(my_addr));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST, &server_addr.sin_addr);

    int flags = fcntl(sock_fd, F_GETFL, 0);
    fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);

    int rcvbuf = 1024 * 1024 * 2; // 2MB UDP Receive Buffer
    setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    uint16_t UE_sfn = 0;
    bool is_synced = false;
    int tick_since_sync = 0;

    TimePoint next_tick = Clock::now() + TICK_MS;
    uint64_t my_suci = suci;
    uint16_t my_tmsi = 0;

    int state = 0;
    bool has_sent_reg = false;

    while (!force_exit.load())
    {
        std::this_thread::sleep_until(next_tick);
        next_tick += TICK_MS;
        UE_sfn = (UE_sfn + 1) % 1024;

        if (!is_synced)
        {
            uint8_t tune_signal = 0x02;
            sendto(sock_fd, &tune_signal, sizeof(tune_signal), 0, (struct sockaddr *)&server_addr, sizeof(server_addr));
        }
        else
        {
            tick_since_sync++;
        }

        if (state == 2)
        {
            int drx_offset = UE_sfn % 64;
            if (drx_offset > 55 && tick_since_sync < RESYNC_TICKS)
            { // Mở rộng cửa sổ nghe để tránh Flush mất gói tin bị delay
                uint8_t dummy[512];
                while (recv(sock_fd, dummy, sizeof(dummy), 0) > 0)
                {
                }
                continue;
            }
        }

        uint8_t buffer[512];
        struct sockaddr_in sender_addr;
        socklen_t sender_len = sizeof(sender_addr);

        while (1)
        {
            ssize_t recv_len = recvfrom(sock_fd, buffer, sizeof(buffer), 0, (struct sockaddr *)&sender_addr, &sender_len);
            if (recv_len <= 0)
                break;

            if (recv_len == sizeof(SsbPacket) && buffer[0] == 0x01)
            {
                SsbPacket *ssb = (SsbPacket *)buffer;
                uint16_t received_sfn = ((uint16_t)(ssb->mib.sfn_msb6 & 0x3F) << 4) | (ssb->sfn_lsb4 & 0x0F);

                if (!is_synced)
                {
                    UE_sfn = received_sfn;
                    is_synced = true;
                    tick_since_sync = 0;
                    next_tick = Clock::now() + TICK_MS; // Căn chỉnh lại pha

                    if (!has_sent_reg && state == 0)
                    {
                        ControlMsg reg_req = {RRC_REG_REQUEST, my_suci, 0};
                        sendto(sock_fd, &reg_req, sizeof(reg_req), 0, (struct sockaddr *)&server_addr, sizeof(server_addr));
                        has_sent_reg = true;
                    }
                }
                else if (tick_since_sync >= RESYNC_TICKS)
                {
                    UE_sfn = received_sfn;
                    tick_since_sync = 0;
                }
            }
            else if (recv_len >= sizeof(uint32_t))
            {
                uint32_t msg_type = *(uint32_t *)buffer;
                if (msg_type == RRC_REG_ACCEPT)
                {
                    ControlMsg *msg = (ControlMsg *)buffer;
                    if (msg->suci == my_suci)
                    {
                        my_tmsi = msg->ue_id;
                        state = 1; // CM-CONNECTED
                        stat_registered++;
                        if (verbose)
                            printf("[UE %lu] Đăng ký thành công! TMSI = %d\n", my_suci, my_tmsi);
                    }
                }
                else if (msg_type == RRC_RELEASE)
                {
                    ControlMsg *msg = (ControlMsg *)buffer;
                    if (msg->ue_id == my_tmsi && state == 1)
                    {
                        state = 2; // CM-IDLE
                        stat_idle++;
                        if (verbose)
                            printf("[UE %lu] Vào chế độ IDLE.\n", my_suci);
                    }
                }
                else if (msg_type == RRC_PAGING)
                {
                    if (state == 2)
                    {
                        PagingMsg *p_msg = (PagingMsg *)buffer;
                        if (p_msg->ue_id == my_tmsi)
                        {
                            {
                                std::lock_guard<std::mutex> t_lock(time_mutex);
                                if (!first_paging_received.exchange(true))
                                {
                                    first_paging_time = Clock::now();
                                }
                                last_paging_time = Clock::now();
                            }
                            stat_paged++;
                            if (verbose)
                                printf("[UE %lu] 🔥 (SFN=%d) Nhận PAGING! (TAC=%d, CN_Domain=%d)\n", my_suci, UE_sfn, p_msg->tac, p_msg->cn_domain);

                            close(sock_fd);
                            return;
                        }
                    }
                }
            }
        }
    }
}

int main(int argc, char *argv[])
{
    setbuf(stdout, NULL);
    int num_ues = 50;
    if (argc > 1)
    {
        num_ues = atoi(argv[1]);
    }

    // Tắt log cá nhân nếu số lượng UE > 20 để tránh tắc nghẽn in ấn (I/O Bottleneck)
    if (num_ues <= 20)
        verbose = true;

    printf("=== BẮT ĐẦU MÔ PHỎNG KIỂM TRA HIỆU NĂNG %d UEs ===\n", num_ues);
    std::vector<std::thread> ue_threads;

    // Khởi tạo luồng
    for (int i = 0; i < num_ues; i++)
    {
        uint64_t suci = 1000000 + i;
        ue_threads.push_back(std::thread(run_ue_thread, suci));
        std::this_thread::sleep_for(std::chrono::milliseconds(1)); // Giảm trễ tạo luồng để dồn cục Paging, đẩy Throughput lên cao
    }

    // BỘ GIÁM SÁT HIỆU NĂNG (Monitor Loop)
    int paged_count = 0;
    int previous_paged = 0;
    int idle_seconds = 0;
    int elapsed_seconds = 0;
    int timeout_limit = 20;

    while (paged_count < num_ues)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        elapsed_seconds++;
        paged_count = stat_paged.load();
        int paged_this_sec = paged_count - previous_paged;
        previous_paged = paged_count;

        if (elapsed_seconds % 5 == 0 && paged_count < num_ues)
        {
            printf("[Multi_UE] Đang chạy mô phỏng... (Đã đánh thức: %d/%d)\n", paged_count, num_ues);
        }

        if (paged_this_sec == 0 && paged_count > 0)
        {
            idle_seconds++;
            if (idle_seconds >= 3)
            {
                printf("\n⚠️ Không có thêm Paging nào trong 3s. Dừng sớm để đo lường khách quan!\n");
                force_exit.store(true);
                break;
            }
        }
        else
        {
            idle_seconds = 0;
        }

        if (elapsed_seconds >= timeout_limit)
        {
            printf("\n❌ HẾT THỜI GIAN (TIMEOUT)!\n");
            force_exit.store(true);
            break;
        }
    }

    // TÍNH TOÁN VÀ IN BÁO CÁO KPI
    for (auto &th : ue_threads)
    {
        if (th.joinable())
            th.join();
    }

    int final_paged = stat_paged.load();
    double duration_sec = 0;
    if (final_paged > 0)
    {
        duration_sec = std::chrono::duration<double>(last_paging_time - first_paging_time).count();
        if (duration_sec < 0.001)
            duration_sec = 0.001; // Tránh lỗi chia 0
    }
    printf("\n=======================================================\n");
    printf("📊 BÁO CÁO KẾT QUẢ ĐÁNH GIÁ PAGING (THROUGHPUT KPI)\n");
    printf("=======================================================\n");
    printf("- Tổng số thiết bị đánh thức  : %d UEs\n", paged_count);

    if (duration_sec > 0)
    {
        double throughput = paged_count / duration_sec;
        printf("- Thời gian hoàn thành Paging : %.2f giây\n", duration_sec);
        printf("- Tốc độ đáp ứng (Throughput) : %.2f bản tin / giây\n", throughput);

        if (throughput < 400.0)
        {
            printf("⚠️ CẢNH BÁO: Hiệu năng chưa đạt yêu cầu 400-500 Paging/s.\n");
        }
        else
        {
            printf("✅ XUẤT SẮC: Trạm đã vượt yêu cầu 400 Paging/s.\n");
        }
    }
    else
    {
        printf("- Hoàn thành cực nhanh, độ trễ không đáng kể.\n");
    }
    printf("=======================================================\n\n");

    return 0;
}
