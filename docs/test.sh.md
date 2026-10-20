# Giải thích quá trình xử lý lỗi khi chạy kiểm thử MapReduce (`test.sh`) trên cụm Hadoop 4 Node

Tài liệu này tổng hợp chi tiết các nguyên nhân và giải pháp kỹ thuật đã thực hiện để khắc phục các lỗi phát sinh khi chạy kịch bản kiểm thử MapReduce WordCount (`test_bigdata/test.sh`) trên cụm 4 máy (`anh24162006-master`, `hoc-slave1`, `nguyen-slave2`, `anh24133004-slave3`) qua Docker & Tailscale VPN.

---

## 1. Kịch bản kiểm thử (`test.sh`) thực hiện những gì?

Khi chạy lệnh `./test.sh` trên máy Master (`anh24162006-master`), hệ thống lần lượt thực hiện 4 bước:
1. Tạo thư mục `/input` trên hệ thống file phân tán HDFS và đẩy file dữ liệu văn bản mẫu (`file1.txt` chứa `"Hello World Hello Hadoop"`) từ máy cục bộ lên các `DataNode`.
2. Khởi chạy chương trình **Hadoop MapReduce WordCount** chuẩn (`hadoop-mapreduce-examples-3.5.0.jar`) thông qua trình quản lý tài nguyên **YARN**.
3. Đọc kết quả đầu ra từ thư mục `/output/part-r-00000` trên HDFS.
4. Dọn dẹp file tạm sau khi hoàn tất.

---

## 2. Vấn đề 1: Lỗi `Excluding datanode 172.18.0.1:19888` khi ghi dữ liệu lên HDFS (`DataStreamer`)

### Hiện tượng
Khi lệnh `hdfs dfs -put` hoặc MapReduce Job bắt đầu đẩy các gói tin (block) lên HDFS, màn hình xuất hiện cảnh báo:
```text
WARN hdfs.DataStreamer: Exception in createBlockOutputStream blk_1073741829_1005
java.io.IOException: Got error, status=ERROR, status message , ack with firstBadLink as 172.18.0.1:19888
WARN hdfs.DataStreamer: Abandoning BP-...: datanode (DatanodeInfoWithStorage[172.18.0.1:19888,DISK]) is bad.
WARN hdfs.DataStreamer: Excluding datanode DatanodeInfoWithStorage[172.18.0.1:19888,DISK]
```

### Nguyên nhân
1. **Hệ số nhân bản (`dfs.replication = 3`) yêu cầu ghi nối tiếp (Pipeline) qua cả 3 máy Slave:**
   - Khi hệ số nhân bản là `3`, HDFS tạo một đường ống truyền dữ liệu nối tiếp (`hoc-slave1` $\rightarrow$ `nguyen-slave2` $\rightarrow$ `anh24133004-slave3`).
   - Do trước đó `NameNode` ghi nhận địa chỉ IP nguồn của các `DataNode` qua cổng NAT Docker Bridge là `172.18.0.1`, khi một `DataNode` (ví dụ `hoc-slave1`) cố gắng chuyển tiếp block sang `172.18.0.1:19888` (`anh24133004-slave3`) mà chưa bật cờ ưu tiên dùng **Hostname** ở phía `DataNode`, nó sẽ gọi nhầm vào chính gateway Docker nội bộ của máy mình (`172.18.0.1`) thay vì gọi qua IP Tailscale của máy `anh24133004-slave3`.
2. **Xử lý tự phục hồi (Fault Tolerance) của HDFS:**
   - Khi liên kết tới `19888` trong đường ống gặp lỗi, cơ chế chịu lỗi của HDFS (`DataStreamer Error Recovery`) tự động loại bỏ node lỗi khỏi pipeline của block đó và ghi thành công vào 2 node còn lại (`hoc-slave1` và `nguyen-slave2`).

### Cách khắc phục đã thực hiện
- Bổ sung cấu hình bắt buộc cả **Client** lẫn **DataNode** sử dụng **Hostname** (ánh xạ qua `/etc/hosts` tới IP Tailscale thật) thay vì IP NAT nội bộ khi truyền block dữ liệu trong `config/hdfs-site.xml`:
  ```xml
  <property>
      <name>dfs.client.use.datanode.hostname</name>
      <value>true</value>
  </property>
  <property>
      <name>dfs.datanode.use.datanode.hostname</name>
      <value>true</value>
  </property>
  <property>
      <name>dfs.replication</name>
      <value>2</value>
  </property>
  ```
- Giảm `dfs.replication` xuống `2` để giảm tải băng thông truyền chéo qua mạng VPN Tailscale và đảm bảo pipeline luôn thành công ngay từ lần đầu tiên.

---

## 3. Vấn đề 2: Job MapReduce bị treo ở `map 0% reduce 0%` và lỗi `NoRouteToHostException` trên YARN `NodeManager`

### Hiện tượng
Khi chạy `hadoop jar ... wordcount /input /output`, tiến trình dừng rất lâu ở trạng thái `RUNNING` (`map 0% reduce 0%`), sau đó báo lỗi:
```text
Container launch failed for container_1792392522567_0001_01_000001 :
java.net.NoRouteToHostException: No route to host from anh24162006-master/172.18.0.2 to hoc-slave1:37205
```

### Nguyên nhân
- Theo mặc định của Apache Hadoop YARN, khi dịch vụ `NodeManager` khởi động trên các máy Slave (`hoc-slave1`, `nguyen-slave2`, `anh24133004-slave3`), tham số `yarn.nodemanager.address` được đặt là `0.0.0.0:0`.
- Số `0` ở cuối có nghĩa là hệ điều hành Linux trong container sẽ **chọn ngẫu nhiên một cổng (ephemeral port)** mỗi lần khởi động (ví dụ: `37205`, `33297`, `46767`).
- Tuy nhiên, các container đang chạy ở chế độ Docker Bridge trên Windows, chỉ những cổng được khai báo tường minh trong mục `ports:` của `docker-compose.yml` mới được mở ra mạng Tailscale bên ngoài. Vì cổng ngẫu nhiên `37205` không được forward trên Docker của máy Slave, máy Master (`ResourceManager`) không thể gửi lệnh khởi tạo Container (`ContainerManagementProtocol`) sang máy Slave được.

### Cách khắc phục đã thực hiện
- **Cố định cổng giao tiếp của `NodeManager` và `Shuffle Handler`** vào đúng các cổng đã được mở sẵn trong `docker-compose.yml` (mà trên các máy Slave không có tiến trình nào sử dụng):
  1. Trong `config/yarn-site.xml`:
     - Đặt `yarn.nodemanager.address = 0.0.0.0:8033` (cố định cổng nhận lệnh điều khiển container của NodeManager ở port `8033`).
     - Đặt `yarn.nodemanager.localizer.address = 0.0.0.0:8030` (cố định cổng tải tài nguyên cục bộ của NodeManager ở port `8030`).
     - Đặt `yarn.nodemanager.webapp.address = 0.0.0.0:8042`.
  2. Trong `config/mapred-site.xml`:
     - Đặt `mapreduce.shuffle.port = 4040` (cố định cổng truyền dữ liệu trung gian giữa pha Map và pha Reduce qua cổng `4040` đã mở sẵn, thay vì cổng mặc định `13562` chưa mở trong Docker).
     - Bật `mapreduce.job.ubertask.enable = true` để tối ưu hóa tốc độ thực thi cho các tác vụ MapReduce nhỏ.
- **Đồng bộ cấu hình và khởi động lại YARN:**
  ```bash
  for h in hoc-slave1 nguyen-slave2 anh24133004-slave3; do
      scp -P 2222 ~/hadoop/etc/hadoop/yarn-site.xml ~/hadoop/etc/hadoop/mapred-site.xml ~/hadoop/etc/hadoop/hdfs-site.xml $h:~/hadoop/etc/hadoop/
  done
  stop-yarn.sh
  start-yarn.sh
  ```
- Sau khi khởi động lại, kiểm tra `yarn node -list` xác nhận cả 3 máy Slave đều lắng nghe cố định ở cổng **`8033`**:
  ```text
  Total Nodes:3
           Node-Id             Node-State
   hoc-slave1:8033                RUNNING
   nguyen-slave2:8033             RUNNING
   anh24133004-slave3:8033        RUNNING
  ```

---

## 4. Vấn đề 3: Thông báo `Retrying connect to server` tới cổng động trên Slave và `JobHistoryServer` (`10020`)

### Hiện tượng
Trong quá trình job đang chạy trên Slave và ngay sau khi job vừa hoàn thành, trên màn hình xuất hiện 2 đoạn thông báo thử kết nối:
1. Lúc đang chạy: `Retrying connect to server: nguyen-slave2/100.106.204.115:36267`
2. Lúc vừa chạy xong: `Redirecting to job history server` và `Retrying connect to server: anh24162006-master/172.18.0.2:10020`

### Nguyên nhân & Giải pháp
1. **Kết nối tới cổng động của `MRAppMaster` trên Slave (`nguyen-slave2:41379`):**
   - Mặc định, tiến trình `MRAppMaster` được YARN khởi tạo bên trong container của máy Slave (`nguyen-slave2`) sẽ mở một cổng RPC ngẫu nhiên (`41379` hoặc `36267`) để báo cáo phần trăm tiến độ trực tiếp cho Client. Vì cổng ngẫu nhiên này không nằm trong danh sách port được mở của `docker-compose.yml`, Client trên Master không kết nối trực tiếp vào được mà phải đợi `MRAppMaster` chạy xong.
   - **Cách khắc phục triệt để:** Cố định dải cổng RPC của `MRAppMaster` trong `config/mapred-site.xml` vào cổng `8031-8032` (đã được mở sẵn trong `docker-compose.yml` và hoàn toàn trống trên các máy Slave):
     ```xml
     <property>
         <name>yarn.app.mapreduce.am.job.client.port-range</name>
         <value>8031-8032</value>
     </property>
     ```
2. **Kết nối tới `JobHistoryServer` (`anh24162006-master:10020`):**
   - Lệnh `start-all.sh` mặc định chỉ khởi động các tiến trình của HDFS và YARN, **không tự động bật `JobHistoryServer`** (lắng nghe ở cổng `10020`). Khi job kết thúc mà Client chưa lấy được báo cáo trực tiếp từ `MRAppMaster`, nó sẽ chuyển hướng sang gọi `anh24162006-master:10020` 30 lần và báo `Connection refused`.
   - **Cách khắc phục triệt để:** Bật dịch vụ `JobHistoryServer` trên máy Master (`anh24162006-master`):
     ```bash
     mapred --daemon start historyserver
     ```

---

## 5. Kết quả thực thi thành công (`./test.sh`)

Sau khi áp dụng đầy đủ các cấu hình trên, kịch bản `./test.sh` đã chạy hoàn tất thành công trên cụm 4 máy (`FinalApplicationStatus=SUCCEEDED`) và xuất ra kết quả đếm từ chính xác từ HDFS:

```text
INFO impl.YarnClientImpl: Application state is completed. FinalApplicationStatus=SUCCEEDED.
INFO mapreduce.Job:  map 100% reduce 100%
INFO mapreduce.Job: Job job_1792402292370_0001 completed successfully
wordcount output:
Hadoop  1
Hello   2
World   1
```

