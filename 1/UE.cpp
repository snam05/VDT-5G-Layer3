#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <chrono>
#include <thread>

#define PORT           5000
#define HOST           "127.0.0.1"
#define RESYNC_TICKS   80     // Hiệu chỉnh lại sau 80 tick = 800 ms
#define UE_ID          10     // ID duy nhất của UE (Ví dụ: 10, 20...) lấy từ file cấu hình của UE

using Clock     = std::chrono::steady_clock;
using Ms        = std::chrono::milliseconds;
using TimePoint = Clock::time_point;

static constexpr Ms TICK_MS{10};   // Chu kỳ mỗi tick = 10 ms

// ============================================================================
// ĐỊNH DẠNG BẢN TIN MIB (phải khớp chính xác với gNodeB)
//   IE_1 (1 byte) : message_id = 0x01
//   IE_2 (1 byte) : sfn_msb6   = 6 bit CAO của SFN (bits 9..4)
//                   → sfn_msb6 = SFN >> 4
//   Tổng = 2 bytes
// ============================================================================
#pragma pack(push, 1)
typedef struct {
    uint8_t message_id;   // IE_1: cố định 0x01
    uint8_t sfn_msb6;     // IE_2: 6 bit cao của SFN (bits 9..4)
} MibContent;

// ============================================================================
// ĐỊNH DẠNG GÓI TIN SSB (phải khớp chính xác với gNodeB)
//   Trường 1 (struct MibContent) : bản tin MIB (2 bytes)
//   Trường 2 (1 byte)            : sfn_lsb4 = 4 bit THẤP của SFN (bits 3..0)
//   Tổng = 3 bytes
//
//   Ghép lại SFN đầy đủ 10 bit:
//     SFN = ((uint16_t)sfn_msb6 << 4) | (sfn_lsb4 & 0x0F)
// ============================================================================
typedef struct {
    int pss;
    int sss;
    MibContent mib;       // Bản tin MIB (chứa 6 bit cao SFN)
    uint8_t    sfn_lsb4;  // 4 bit thấp của SFN (bits 3..0)
} SsbPacket;

#pragma pack(pop)

int main() {
    // -----------------------------------------------------------------------
    // 1. Tạo UDP socket
    // -----------------------------------------------------------------------
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock_fd < 0) {
        perror("[UE] Tạo socket thất bại");
        return EXIT_FAILURE;
    }

    // -----------------------------------------------------------------------
    // 2. Khai báo địa chỉ gNodeB (Server) để gửi RRC Request
    // -----------------------------------------------------------------------
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, HOST, &server_addr.sin_addr);

    // -----------------------------------------------------------------------
    // 3. Đặt socket sang chế độ NON-BLOCKING
    //    → UE vừa tự tăng SFN mỗi 10ms, vừa poll gói tin mà không bị chặn
    // -----------------------------------------------------------------------
    int flags = fcntl(sock_fd, F_GETFL, 0);
    fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);

    // -----------------------------------------------------------------------
    // 4. Trạng thái nội bộ của UE
    // -----------------------------------------------------------------------
    uint16_t  UE_sfn         = 0;
    bool      is_synced       = false;
    int       tick_since_sync = 0;          // số tick kể từ lần đồng bộ / hiệu chỉnh gần nhất
    TimePoint next_tick       = Clock::now() + TICK_MS;  // mốc thời gian tuyệt đối của tick đầu tiên

    printf("[UE] Khởi chạy thành công. Giả lập mở ăng-ten dò sóng (Cell Search)...\n");

    while (1) {
        // -------------------------------------------------------------------
        // Tick 10ms: ngủ đúng đến mốc tuyệt đối → không có drift dù xử lý mất vài ms
        // -------------------------------------------------------------------
        std::this_thread::sleep_until(next_tick);
        next_tick += TICK_MS;               // lên lịch tick kế tiếp

        UE_sfn = (UE_sfn + 1) % 1024;

        uint8_t tune_signal = 0x02;

        if (!is_synced) {
            // Gửi tín hiệu giả lập (0x02) lên gNodeB để Server biết Port của UE
            sendto(sock_fd, &tune_signal, sizeof(tune_signal), 0,
                   (struct sockaddr *)&server_addr, sizeof(server_addr));   
        }

        if (is_synced) {
            tick_since_sync++;
        }

        // Nếu quá hạn mốc hiệu chỉnh 800ms mà chờ thêm 400ms (40 ticks) vẫn không thấy SSB -> Mất sóng
        if (is_synced && tick_since_sync > RESYNC_TICKS + 40) {
            printf("[UE] CẢNH BÁO: Mất tín hiệu đồng bộ (Vượt quá thời gian chịu đựng)!\n");
            is_synced = false;  // mất đồng bộ

            // Gửi lại tín hiệu dò sóng
            sendto(sock_fd, &tune_signal, sizeof(tune_signal), 0,
                   (struct sockaddr *)&server_addr, sizeof(server_addr));
        }

        // -------------------------------------------------------------------
        // Poll: kiểm tra có gói MIB nào đến trong tick này không
        // -------------------------------------------------------------------
        SsbPacket ssb;
        struct sockaddr_in sender_addr;
        socklen_t sender_len = sizeof(sender_addr);

        ssize_t recv_len = recvfrom(sock_fd, &ssb, sizeof(ssb), 0,
                                    (struct sockaddr *)&sender_addr, &sender_len);

        if (recv_len != sizeof(SsbPacket)) {
            continue;
        }

        // Kiểm tra IE_1 trong MIB: chỉ xử lý bản tin hợp lệ
        if (ssb.mib.message_id != 0x01) {
            continue;
        }

        // Nếu đã đồng bộ và chưa đến kỳ lấy mẫu (ví dụ dưới 800ms), 
        // UE chỉ xả buffer của HĐH (để không bị đầy) rồi bỏ qua, DÙNG BỘ ĐẾM NỘI BỘ
        if (is_synced && tick_since_sync < RESYNC_TICKS) {
            continue;
        }

        // -- Ghép lại SFN đầy đủ 10 bit từ MIB (6 bit cao) và sfn_lsb4 (4 bit thấp) --
        uint16_t received_sfn = ((uint16_t)(ssb.mib.sfn_msb6 & 0x3F) << 4)
                                | (ssb.sfn_lsb4 & 0x0F);
        printf("[UE] Nhận SSB: MIB.sfn_msb6 = 0x%02X (%d) | sfn_lsb4 = 0x%X (%d) -> SFN ghép = %d\n",
               ssb.mib.sfn_msb6, ssb.mib.sfn_msb6,
               ssb.sfn_lsb4,     ssb.sfn_lsb4,
               received_sfn);

        // -------------------------------------------------------------------
        // TRƯỜNG HỢP 1: Chưa đồng bộ → lấy SFN từ gNodeB ngay lập tức
        // -------------------------------------------------------------------
        if (!is_synced) {
            UE_sfn          = received_sfn;
            is_synced       = true;
            tick_since_sync = 0;
            next_tick       = Clock::now() + TICK_MS;  // Cập nhật lại mốc tuyệt đối cho tick kế tiếp
            printf("[UE] *** ĐỒNG BỘ THÀNH CÔNG! *** UE_sfn = %d\n", UE_sfn);
        }
        // -------------------------------------------------------------------
        // TRƯỜNG HỢP 2: Đến lúc cần hiệu chỉnh (tick >= 800ms)
        // -------------------------------------------------------------------
        else if (tick_since_sync >= RESYNC_TICKS) {
            printf("[UE] [Hiệu chỉnh định kỳ 800ms] SFN nội bộ: %d  →  SFN mạng: %d\n",
                   UE_sfn, received_sfn);
            UE_sfn          = received_sfn;
            tick_since_sync = 0;
        }
    }

    close(sock_fd);
    return 0;
}
