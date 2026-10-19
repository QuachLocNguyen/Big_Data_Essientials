# Giải thích quá trình xử lý và khởi chạy cụm Hadoop (`start-all.sh`) trên Docker & Tailscale

Tài liệu này giải thích chi tiết các vấn đề kỹ thuật đã phát sinh khi khởi chạy cụm Hadoop 4 node (`anh24162006-master`, `hoc-slave1`, `nguyen-slave2`, `anh24133004-slave3`) qua Docker + Tailscale VPN và cách khắc phục triệt để để hệ thống nhận đủ **3 DataNode (2.04 TB)** và **3 NodeManager**.

---

## 1. Vấn đề 1: Lỗi `Connection timed out` ở cổng SSH (Port 22) và chặn gói tin trên Tailscale

### Hiện tượng
- Khi chạy `start-all.sh` lần đầu, máy Master không thể SSH sang 3 máy Slave (`port 22: Connection timed out`).
- Máy Master `ping` được 3 máy Slave, nhưng 3 máy Slave không `ping` hoặc kết nối ngược về máy Master được.

### Nguyên nhân
1. **Xung đột cổng 22 trên Host:** Cổng `22` mặc định trên Windows thường bị dịch vụ OpenSSH của hệ điều hành hoặc tường lửa chặn.
2. **Chính sách ACL (Access Control List) mặc định của Tailscale:** Khi mời nhiều tài khoản email khác nhau (`duchoc2006@`, `quachlocnguyen@`, `JPDeep4122@`) vào mạng Tailnet của Admin (`ledanghoanganh@`), quyền mặc định `autogroup:self` chỉ cho phép Admin gọi tới các máy Member, nhưng chặn chiều từ Member gọi ngược về Admin và gọi chéo giữa các Member.

### Cách khắc phục đã thực hiện
1. **Đổi cổng SSH sang `2222`:**
   - Trong `docker-compose.yml`: Mở port `"2222:2222"`.
   - Trong `Dockerfile` và `entrypoint.sh`: Cấu hình `/etc/ssh/sshd_config` lắng nghe ở `Port 2222`, `/etc/ssh/ssh_config` mặc định gọi `Port 2222`, và thêm `export HADOOP_SSH_OPTS="-p 2222"` vào `hadoop-env.sh`.
2. **Mở quyền giao tiếp 2 chiều trên Tailscale ACLs:**
   - Cập nhật Access Controls trên Tailscale Admin Console (`src: ["*"], dst: ["*:*"]`) và mở Firewall Windows cho dải IP Tailscale (`100.64.0.0/10`).
3. **Trao đổi SSH Key (`ssh-copy-id -p 2222`):**
   - Sao chép khóa công khai (`id_rsa.pub`) từ `anh24162006-master` sang `~/.ssh/authorized_keys` của 3 máy Slave để đăng nhập SSH không cần mật khẩu.

---

## 2. Vấn đề 2: `DataNode` bị `NameNode` từ chối đăng ký do cơ chế kiểm tra Reverse DNS của Docker NAT

### Hiện tượng
- Sau khi `start-all.sh` chạy xong, kiểm tra `jps` thấy tiến trình `DataNode` đã bật, nhưng chạy `hdfs dfsadmin -report` trên Master lại báo `Configured Capacity: 0 (0 B)` (không có DataNode nào kết nối thành công).
- Kiểm tra file log `~/hadoop/logs/*datanode*.log` trên các máy Slave thấy lỗi:
  ```text
  Datanode denied communication with namenode because hostname cannot be resolved (ip=172.18.0.1, hostname=172.18.0.1)
  ```

### Nguyên nhân
- Khi container `DataNode` từ máy Slave kết nối qua Tailscale tới cổng `9000` của máy Windows Master, **Docker Desktop trên máy Master** thực hiện chuyển tiếp cổng (NAT/Port Forwarding) thông qua card mạng cầu nối Docker Bridge (`172.18.0.1`) vào container `anh24162006-master` (`172.18.0.2`).
- Do đó, `NameNode` nhìn thấy địa chỉ IP nguồn (Remote IP) của gói tin là `172.18.0.1`.
- Theo mặc định, HDFS bật cờ kiểm tra tên miền ngược (`dfs.namenode.datanode.registration.ip-hostname-check = true`). Khi `NameNode` tra cứu ngược IP `172.18.0.1` không ra tên miền hợp lệ, nó lập tức từ chối cho `DataNode` đăng ký.

### Cách khắc phục đã thực hiện
- Bổ sung cấu hình tắt kiểm tra Reverse DNS khi đăng ký DataNode vào file `config/hdfs-site.xml`:
  ```xml
  <property>
      <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
      <value>false</value>
  </property>
  ```

---

## 3. Vấn đề 3: 3 máy Slave ghi đè nhau trên `NameNode` (`Live datanodes: 1` thay vì `3`)

### Hiện tượng
- Sau khi tắt kiểm tra IP-hostname, cả 3 máy Slave đều đăng ký được với `NameNode`, nhưng `hdfs dfsadmin -report` chỉ hiển thị **1 DataNode duy nhất (`Live datanodes (1)`)** và tên máy liên tục bị thay đổi qua lại giữa `hoc-slave1`, `nguyen-slave2`, `anh24133004-slave3`.
- Trong log của `NameNode` xuất hiện dòng:
  ```text
  BLOCK* registerDatanode: 172.18.0.1:9866
  Removing a node: /default-rack/172.18.0.1:9866
  Adding a new node: /default-rack/172.18.0.1:9866
  ```

### Nguyên nhân
- Trong mã nguồn của Hadoop (`DatanodeManager.registerDatanode` và `Host2NodesMap`), `NameNode` định danh kết nối mạng của mỗi `DataNode` bằng cặp **`(Remote_IP : xferPort)`**.
- Vì cả 3 máy Slave đi qua Docker Bridge NAT của máy Master đều mang cùng `Remote_IP` là `172.18.0.1`, và mặc định cả 3 đều dùng chung cổng truyền dữ liệu `xferPort = 9866` (`dfs.datanode.address = 0.0.0.0:9866`), nên `NameNode` thấy cả 3 máy đều có định danh là **`172.18.0.1:9866`**.
- Khi Slave 2 đăng ký với định danh `172.18.0.1:9866`, `NameNode` tưởng rằng node cũ vừa khởi động lại với UUID mới nên xóa Slave 1 đi để thay bằng Slave 2, và Slave 3 lại xóa Slave 2.

### Cách khắc phục đã thực hiện
- Trong khi giao tiếp dữ liệu thực tế giữa các node đã được cấu hình dùng **Hostname** (`dfs.client.use.datanode.hostname = true` và `dfs.datanode.use.datanode.hostname = true`), để `NameNode` phân biệt được 3 `DataNode` phía sau cùng một cổng NAT `172.18.0.1`, mỗi Slave chỉ cần sử dụng một `xferPort` khác nhau (tận dụng các port đã được mở sẵn trong `docker-compose.yml`):
  - **`hoc-slave1`**: Giữ nguyên port mặc định `9866` (`172.18.0.1:9866`).
  - **`nguyen-slave2`**: Đặt `dfs.datanode.address=0.0.0.0:10020` (`172.18.0.1:10020`).
  - **`anh24133004-slave3`**: Đặt `dfs.datanode.address=0.0.0.0:19888` (`172.18.0.1:19888`).
- Cấu hình này đã được tự động hóa trong `entrypoint.sh` và `hadoop-env.sh` trên các node Slave:
  ```bash
  if [ "$(hostname)" = "nguyen-slave2" ]; then
      grep -q "HDFS_DATANODE_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh || \
          echo 'export HDFS_DATANODE_OPTS="-Ddfs.datanode.address=0.0.0.0:10020"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh
  elif [ "$(hostname)" = "anh24133004-slave3" ]; then
      grep -q "HDFS_DATANODE_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh || \
          echo 'export HDFS_DATANODE_OPTS="-Ddfs.datanode.address=0.0.0.0:19888"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh
  fi
  ```

---

## 4. Vấn đề 4: Thiếu file cấu hình trên `anh24133004-slave3`

### Hiện tượng
- Ban đầu máy `anh24133004-slave3` chỉ chạy `NodeManager` ở trạng thái thử lại kết nối (`0.0.0.0:8031`) và `DataNode` tự tắt ngay sau khi bật với lỗi `No services to connect, missing NameNode address`.

### Nguyên nhân
- Trên máy `anh24133004-slave3`, đường dẫn mount `volumes` trong `docker-compose.yml` trước đó trỏ vào `/home/hadoophoanganh24162006/...` trong khi user thực tế trong container là `/home/hadoopanh24162006`, dẫn đến thư mục `~/hadoop/etc/hadoop/` vẫn là các file cấu hình rỗng mặc định của Hadoop.

### Cách khắc phục đã thực hiện
- Sử dụng lệnh `scp -P 2222` từ máy `anh24162006-master` để đồng bộ trực tiếp toàn bộ các file cấu hình chuẩn (`core-site.xml`, `hdfs-site.xml`, `yarn-site.xml`, `mapred-site.xml`, `workers`) sang thư mục `~/hadoop/etc/hadoop/` của `anh24133004-slave3`.

---

## 5. Kết quả đạt được sau khi hoàn tất

Sau khi áp dụng đầy đủ các bước trên và chạy `start-all.sh`:
1. **Lệnh `jps`:**
   - Máy `anh24162006-master`: Chạy đủ `NameNode`, `SecondaryNameNode`, `ResourceManager`.
   - 3 máy `hoc-slave1`, `nguyen-slave2`, `anh24133004-slave3`: Đều chạy đủ `DataNode` và `NodeManager`.
2. **Lệnh `hdfs dfsadmin -report`:**
   - Báo cáo **`Live datanodes (3)`** hoạt động bình thường với tổng dung lượng lưu trữ phân tán **2.04 TB**.
3. **Lệnh `yarn node -list`:**
   - Báo cáo **`Total Nodes: 3`** (`hoc-slave1`, `nguyen-slave2`, `anh24133004-slave3`) đều ở trạng thái **`RUNNING`**.

