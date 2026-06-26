# Tài liệu Hướng dẫn và Giải thích Chi tiết Mã Nguồn 5G (System Workflow)

Tài liệu này cung cấp phần giải thích **chi tiết từng dòng/khối lệnh** cho hệ thống mô phỏng mạng 5G (gồm AMF, gNodeB, Multi_UE). Tài liệu được thiết kế làm **Tài liệu sử dụng và bàn giao hệ thống**, giúp bất kỳ ai đọc vào cũng có thể hiểu được luồng đi của dữ liệu và ý nghĩa của các dòng code.

---

## 1. Thành phần AMF (AMF.cpp) - Core Network
AMF đóng vai trò quản lý đăng ký thiết bị, cấp phát định danh an toàn (TMSI) và ra lệnh gọi thiết bị (Paging).

### 1.1 Khai báo và Cấu trúc dữ liệu
```cpp
#include <stdio.h> // ... (Các thư viện chuẩn)
#define PORT_TCP 6000     // Cổng TCP để giao tiếp với gNodeB
#define HOST "127.0.0.1"  // Địa chỉ IP của AMF

// Các loại bản tin NGAP (Giao thức mạng lõi)
#define NGAP_INITIAL_UE_MSG 1 // Lệnh xin đăng ký mạng của UE
#define NGAP_REG_ACCEPT 2     // Lệnh chấp nhận đăng ký
#define NGAP_UE_CONTEXT_REL 3 // Lệnh giải phóng kết nối (Về IDLE)
#define NGAP_PAGING 100       // Lệnh Paging (Đánh thức thiết bị)

#pragma pack(push, 1) // Ép trình biên dịch không chèn byte trống (padding), giúp cấu trúc fix cứng 16-byte
typedef struct {
    uint32_t message_type; // Loại bản tin (NGAP_...)
    uint64_t suci;         // Mã định danh thiết bị gốc (ẩn danh SIM)
    uint32_t ue_id;        // Mã TMSI (định danh tạm thời được AMF cấp)
} ControlMsg;

typedef struct {
    uint32_t message_type; // Loại bản tin
    uint32_t ue_id;        // Mã TMSI của thiết bị cần gọi
    uint32_t tac;          // Tracking Area Code (Mã khu vực)
    uint32_t cn_domain;    // Core Network Domain
} PagingMsg;
#pragma pack(pop)
```
**Giải thích**: Dùng `#pragma pack(push, 1)` để cố định kích thước gói tin gửi qua TCP là đúng 16 byte. Điều này giúp ngăn chặn lỗi "trượt khung" (misalignment) khi TCP nhận dữ liệu liên tục.

### 1.2 Hàm quản lý vòng đời UE (`handle_ue_lifecycle`)
```cpp
void handle_ue_lifecycle(int sock_fd, uint16_t allocated_tmsi) {
    // 1. Chờ 2s mô phỏng việc người dùng lướt web xong, không dùng mạng nữa
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 2. Gửi lệnh NGAP_UE_CONTEXT_REL yêu cầu gNodeB cắt sóng UE này, cho nó về ngủ (IDLE)
    ControlMsg rel_msg = {NGAP_UE_CONTEXT_REL, 0, allocated_tmsi};
    send(sock_fd, &rel_msg, sizeof(rel_msg), 0);
    
    // 3. Chờ 3s mô phỏng thiết bị đang trong túi quần (trạng thái IDLE)
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 4. Có cuộc gọi (hoặc tin nhắn Zalo) tới, AMF tạo bản tin NGAP_PAGING
    PagingMsg paging_msg = {NGAP_PAGING, allocated_tmsi, 100, 101};
    send(sock_fd, &paging_msg, sizeof(paging_msg), 0); // Đẩy Paging xuống gNodeB
}
```
**Giải thích**: Hàm này là kịch bản chạy riêng cho **từng UE**. Nó định tuyến việc kết nối, nghỉ ngơi, và bị gọi dậy. Hàng ngàn UE sẽ có hàng ngàn luồng chạy độc lập nhưng dùng chung một `sock_fd` duy nhất.

### 1.3 Hàm Main (Vòng lặp TCP Client)
```cpp
int main() {
    // ... (Khởi tạo socket TCP đóng vai trò Client kết nối tới gNodeB)
    while (connect(sock_fd, ...) < 0) { sleep(1); } // Cứ 1s thử lại nếu gNodeB chưa bật

    ControlMsg msg;
    // Liên tục đọc gói tin 16-byte từ TCP
    while (recv(sock_fd, &msg, sizeof(msg), 0) > 0) {
        if (msg.message_type == NGAP_INITIAL_UE_MSG) { // Nếu có UE xin vào mạng
            uint16_t allocated_tmsi = tmsi_allocator++; // Tạo 1 mã TMSI mới (bắt đầu từ 10)
            
            // Phản hồi chấp nhận kèm mã TMSI
            ControlMsg accept_msg = {NGAP_REG_ACCEPT, msg.suci, allocated_tmsi};
            send(sock_fd, &accept_msg, sizeof(accept_msg), 0);

            // Bật 1 luồng riêng biệt chạy kịch bản 5s (như ở mục 1.2) cho UE này
            std::thread(handle_ue_lifecycle, sock_fd, allocated_tmsi).detach();
        }
    }
}
```

---

## 2. Thành phần gNodeB (gNodeB.cpp) - Trạm Phát Sóng
Đóng vai trò cầu nối (Router), gNodeB chuyển đổi các bản tin RRC (UDP không dây) sang NGAP (TCP có dây) và ngược lại.

### 2.1 Quản lý Thiết Bị
```cpp
// Danh sách các IP:Port (UDP) của các máy điện thoại (UE) đang lảng vảng gần trạm
std::vector<struct sockaddr_in> air_interfaces; 
std::mutex air_mutex; // Khóa bảo vệ danh sách trên

// Bản đồ định tuyến: Khi có lệnh TCP từ Core, phải biết gửi về IP/Port UDP nào
std::map<uint64_t, struct sockaddr_in> context_by_suci; // Bản đồ ánh xạ mã gốc (SUCI) -> IP/Port
std::map<uint32_t, struct sockaddr_in> context_by_tmsi; // Bản đồ ánh xạ mã tạm (TMSI) -> IP/Port
```

### 2.2 Luồng lắng nghe TCP từ Core Network (`tcp_listener_thread`)
```cpp
void tcp_listener_thread() {
    // Tạo Server Socket ở Port 6000 chờ AMF kết nối vào
    int client_fd = accept(server_fd, NULL, NULL); 
    
    char buffer[16];
    // recv(..., MSG_WAITALL) ép HĐH gom đủ 16 byte mới xử lý để chống rách mảnh
    while (recv(client_fd, buffer, 16, MSG_WAITALL) == 16) {
        uint32_t msg_type = *(uint32_t *)buffer; // Đọc 4 byte đầu tiên xem là lệnh gì
        
        if (msg_type == NGAP_REG_ACCEPT) {
            // Tra vào map lấy ra IP UDP của máy có suci tương ứng
            struct sockaddr_in ue_addr = context_by_suci[msg->suci];
            context_by_tmsi[msg->ue_id] = ue_addr; // Lưu thêm TMSI vào map
            
            // Bắn gói tin Chấp nhận (RRC_REG_ACCEPT) qua không khí (UDP)
            ControlMsg rrc_msg = {RRC_REG_ACCEPT, msg->suci, msg->ue_id};
            sendto(sock_udp, &rrc_msg, sizeof(rrc_msg), 0, ...);
        }
        else if (msg_type == NGAP_UE_CONTEXT_REL) {
            // Nếu có lệnh rút sóng, xóa nó khỏi danh sách
            context_by_tmsi.erase(msg->ue_id);
            // Bắn lệnh báo về IDLE cho thiết bị qua UDP
        }
        else if (msg_type == NGAP_PAGING) {
            // RẤT QUAN TRỌNG: Gói Paging không bắn đi ngay! 
            // Nó bị tống vào kho chờ (paging_queue) để chờ đến chu kỳ gom sóng rồi mới vãi ra.
            std::lock_guard<std::mutex> lock(queue_mutex);
            paging_queue.push_back(*msg);
        }
    }
}
```

### 2.3 Luồng vô tuyến 5G: Nhịp SFN và Paging (`main`)
```cpp
uint16_t gNodeB_sfn = 0; // Số khung vô tuyến (0 - 1023)
TimePoint next_tick = Clock::now() + TICK_MS; // Mỗi Tick là 10 mili-giây (10ms)

while (1) {
    std::this_thread::sleep_until(next_tick); // Giữ nhịp độ chính xác tuyệt đối 10ms
    next_tick += TICK_MS;

    // Kênh gom Paging: Mỗi 64 SFN (640ms), lôi hết hàng trong kho ra xử lý
    if (gNodeB_sfn % 64 == 0 && !paging_queue.empty()) {
        local_queue = std::move(paging_queue); // Lấy toàn bộ hàng khỏi kho
        paging_queue.clear();
    }

    // Xả hàng Paging
    if (!local_queue.empty()) {
        for (size_t i = 0; i < local_queue.size(); i++) { // Duyệt từng lệnh Paging (Zalo call)
            // Phát sóng QUẢNG BÁ (Broadcast): Ném lệnh này cho TOÀN BỘ điện thoại trong xóm
            for (auto &addr : air_interfaces) {
                sendto(sock_udp, &p_msg, sizeof(p_msg), 0, (struct sockaddr *)&addr, sizeof(addr));
            }
            usleep(20); // CHỐNG NGHẼN: Ngủ 20 micro-giây để OS Buffer không bị nghẹt do vòng lặp gửi quá nhanh
        }
    }
    if (tick_count >= MIB_CYCLE) {
        SsbPacket ssb;
        ssb.mib.message_id = 0x01; // ...
        for (auto &addr : air_interfaces) sendto(...); // Quảng bá tín hiệu đồng bộ
    }
    
    gNodeB_sfn = (gNodeB_sfn + 1) % 1024; // Tăng SFN, tuần hoàn từ 0 đến 1023
}
```

---

## 3. Thành phần Multi_UE (Multi_UE.cpp) - Khối Người Dùng
Mô phỏng hàng loạt thiết bị điện thoại chạy ngầm cùng lúc để đánh giá mức chịu tải (Throughput).

### 3.1 Vòng lặp từng điện thoại (`run_ue_thread`)
```cpp
void run_ue_thread(uint64_t suci) {
    int sock_fd = socket(AF_INET, SOCK_DGRAM, 0); // Mở 1 port UDP ngẫu nhiên
    fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK);  // Cài đặt NON-BLOCK: hàm recv không bị chặn treo máy

    int rcvbuf = 1024 * 1024 * 2; // Ép đệm nhận lên 2MB để tránh tụt (drop) gói UDP lúc Paging ồ ạt
    setsockopt(sock_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    while (!force_exit.load()) {
        std::this_thread::sleep_until(next_tick); // Mỗi điện thoại tự đếm nhịp 10ms nội bộ

        // Nếu chưa bắt được tín hiệu trạm, bắn gói kích sóng 0x02 liên tục
        if (!is_synced) { sendto(..., tune_signal, ...); }

        // MÔ PHỎNG CHẾ ĐỘ NGỦ (DRX SLEEP) KHI Ở TRẠNG THÁI IDLE (state == 2)
        if (state == 2) {
            int drx_offset = UE_sfn % 64;
            // Nếu offset > 55 tức là ngoài "Khung giờ vàng mở mắt nghe loa", 
            // điện thoại nhắm mắt, nhận dữ liệu vào thùng rác (dummy) rồi bỏ qua ngay lập tức.
            if (drx_offset > 55 && tick_since_sync < RESYNC_TICKS) { 
                uint8_t dummy[512];
                while (recv(sock_fd, dummy, ...) > 0) {} 
                continue; // Cắt ngang vòng lặp, đi ngủ tiếp
            }
        }

        // Khung giờ mở mắt: Đọc gói tin UDP đến
        while (1) {
            ssize_t recv_len = recvfrom(sock_fd, buffer, ...);
            if (recv_len <= 0) break; // Không có gì thì thoát do Non-block
            
            // Xử lý gói đồng bộ SSB -> Đồng bộ hóa đồng hồ UE_sfn trùng với gNodeB_sfn
            if (recv_len == sizeof(SsbPacket)) { UE_sfn = received_sfn; is_synced = true; }
            
            else if (msg_type == RRC_PAGING) {
                if (state == 2) { // Chỉ bắt máy nếu đang ngủ
                    PagingMsg *p_msg = (PagingMsg *)buffer;
                    if (p_msg->ue_id == my_tmsi) { // LOA CÓ GỌI ĐÚNG TÊN MÌNH KHÔNG?
                        stat_paged++; // Đánh dấu điểm cộng
                        close(sock_fd);
                        return; // Hoàn tất vòng đời, luồng tự hủy!
                    }
                }
            }
        }
    }
}
```

### 3.2 Luồng điều khiển tổng (`main`)
```cpp
int main(int argc, char *argv[]) {
    // Vòng lặp đẻ ra N (vd: 50, 500) luồng điện thoại
    for (int i = 0; i < num_ues; i++) {
        uint64_t suci = 1000000 + i;
        ue_threads.push_back(std::thread(run_ue_thread, suci));
        std::this_thread::sleep_for(std::chrono::milliseconds(1)); // Nhả 1ms cho OS cấp phát tài nguyên luồng
    }

    // Luồng giám đốc: Đứng nhòm
    while (paged_count < num_ues) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        paged_count = stat_paged.load(); // Cập nhật số lượng thiết bị đã tỉnh giấc
        
        // Điều kiện kết thúc sớm: 3 giây trôi qua mà không có thêm máy nào tỉnh dậy -> Dừng luôn cho lẹ
        if (paged_this_sec == 0 && paged_count > 0) {
            idle_seconds++;
            if (idle_seconds >= 3) { force_exit.store(true); break; }
        }
    }

    // BÁO CÁO THỐNG KÊ (Throughput)
    double duration_sec = std::chrono::duration<double>(last_paging_time - first_paging_time).count();
    double throughput = paged_count / duration_sec;
    printf("- Tốc độ đáp ứng (Throughput) : %.2f bản tin / giây\n", throughput);
}
```
**Giải thích**: Hàm `main` không tham gia xử lý logic mạng mà chỉ tung ra các luồng, sau đó đóng vai trò đo đếm thời gian từ lúc nhận gói Paging đầu tiên (`first_paging_time`) tới lúc gói Paging cuối cùng được xác nhận (`last_paging_time`), từ đó tính ra tốc độ KPI Throughput Paging thực tế của hệ thống giả lập.