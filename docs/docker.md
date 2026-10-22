# Hướng dẫn chi tiết về Docker trong hệ sinh thái Big Data: Dockerfile, docker-compose.yml và entrypoint.sh

Tài liệu này được biên soạn nhằm giải thích toàn diện và chi tiết từng dòng lệnh, từng tham số trong các file cấu hình Docker của dự án:
1. `Dockerfile` (Bản thiết kế xây dựng Image hệ điều hành và môi trường Big Data).
2. `docker-compose.yml` (Bản điều phối tài nguyên, mạng, cổng giao tiếp và ổ đĩa cho Container).
3. `entrypoint.sh` (Kịch bản khởi động khi Container bắt đầu chạy).

Đồng thời, tài liệu sẽ giúp bạn hiểu rõ cơ chế bên dưới khi gõ lệnh:
```bash
docker compose up -d --build
```

---

## 1. Cơ chế tổng quan: Khi chạy `docker compose up -d --build`, chuyện gì xảy ra?

- **`--build`**: Yêu cầu Docker kiểm tra và dịch lại file `Dockerfile` để đóng gói một Docker Image có tên là `bigdata-ecosystem:latest`. Quá trình này tải base image Ubuntu, cài đặt Java, Python, SSH và bung các bộ cài Hadoop, Spark, Hive đã chuẩn bị sẵn trong thư mục `offline_pkgs/`.
- **`up`**: Lấy Image vừa tạo ở trên, kết hợp với các chỉ số cổng (`ports`), ổ đĩa (`volumes`), biến môi trường (`environment`), bảng phân giải tên miền (`extra_hosts`) trong file `docker-compose.yml` để tạo ra một **Container** (máy ảo thu nhỏ).
- **`-d` (Detached mode)**: Chạy container ở chế độ nền (background), trả lại quyền điều khiển terminal cho bạn thay vì chiếm toàn bộ màn hình console.
- **Khởi động Container**: Sau khi container được cấp phát không gian mạng và ổ đĩa, tiến trình đầu tiên (PID 1) mà nó thực thi chính là kịch bản `entrypoint.sh`. Kịch bản này kiểm tra quyền, bật dịch vụ SSH, format NameNode (nếu là máy Master lần đầu chạy), và duy trì container luôn ở trạng thái `RUNNING` bằng lệnh `tail -f /dev/null`.

---

## 2. Giải thích chi tiết từng dòng lệnh trong `Dockerfile`

File `Dockerfile` là tập hợp các chỉ thị để tạo ra môi trường chuẩn cho toàn bộ 4 máy tính trong cụm.

```dockerfile
FROM ubuntu:24.04
```
- **Ý nghĩa**: Sử dụng hệ điều hành nền là **Ubuntu 24.04 LTS (Noble Numbat)** chính thức từ Docker Hub. Đây là bản phân phối Linux mới nhất, độ ổn định cao, bảo mật tốt.

```dockerfile
ENV DEBIAN_FRONTEND=noninteractive
```
- **Ý nghĩa**: Thiết lập biến môi trường chỉ thị cho trình quản lý gói `apt` không hiện các bảng hỏi tương tác người dùng (như chọn múi giờ, chọn ngôn ngữ bàn phím), giúp quá trình `docker build` diễn ra tự động 100% không bị treo đợi người dùng bấm phím.

```dockerfile
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk openssh-server openssh-client python3 python3-pip \
    iputils-ping net-tools nano curl sudo \
    && rm -rf /var/lib/apt/lists/*
```
- **`apt-get update`**: Cập nhật danh sách các package mới nhất từ server Ubuntu.
- **`apt-get install -y --no-install-recommends`**: Cài đặt các gói phần mềm cần thiết mà không tải kèm các gói đề xuất không bắt buộc (giúp giảm dung lượng image).
  - `openjdk-17-jdk`: Bộ máy Java bắt buộc để chạy Hadoop 3.5.0, Spark 4.2.0 và Hive 4.1.0.
  - `openssh-server`, `openssh-client`: Cung cấp dịch vụ SSH để máy Master có thể đăng nhập không cần mật khẩu sang các máy Slave nhằm gửi lệnh `start-all.sh`, `stop-all.sh`, phân phối công việc MapReduce.
  - `python3`, `python3-pip`: Cần thiết để thực thi các kịch bản PySpark (WordCount, WordMean, WordMedian, WordStdDev).
  - `iputils-ping`, `net-tools`, `nano`, `curl`: Các công cụ mạng và tiện ích terminal để kiểm tra kết nối (`ping`), xem cổng mở (`netstat`), chỉnh sửa nhanh file (`nano`), tải dữ liệu (`curl`).
  - `sudo`: Cấp quyền quản trị tối cao cho user nội bộ khi cần thao tác hệ thống.
- **`rm -rf /var/lib/apt/lists/*`**: Xóa toàn bộ file cache tạm sau khi cài đặt xong để tối ưu kích thước Docker Image.

```dockerfile
# User chung cho toàn cụm
ARG HADOOP_USER=hadoopanh24162006
RUN useradd -m -s /bin/bash ${HADOOP_USER} && \
    echo "${HADOOP_USER}:hadoopanh24162006" | chpasswd && \
    usermod -aG sudo ${HADOOP_USER} && \
    echo "${HADOOP_USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers
```
- **`ARG HADOOP_USER=hadoopanh24162006`**: Định nghĩa biến build để đặt tên tài khoản người dùng chuẩn cho toàn bộ 4 máy tính.
- **`useradd -m -s /bin/bash ${HADOOP_USER}`**: Tạo tài khoản Linux mới, kèm thư mục cá nhân `/home/hadoopanh24162006` (`-m`) và shell mặc định là `/bin/bash` (`-s`).
- **`echo "${HADOOP_USER}:hadoopanh24162006" | chpasswd`**: Đặt mật khẩu đăng nhập cho user này là `hadoopanh24162006`.
- **`usermod -aG sudo ${HADOOP_USER}`**: Thêm user vào nhóm `sudo`.
- **`echo "${HADOOP_USER} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers`**: Cấu hình file sudoers để user này có thể thực thi các lệnh quản trị bằng `sudo` mà không bị hỏi lại mật khẩu (rất tiện khi chạy script tự động).

```dockerfile
# Cấu hình SSH (sử dụng port 2222)
RUN mkdir -p /run/sshd && \
    su - ${HADOOP_USER} -c "ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 600 ~/.ssh/authorized_keys" && \
    echo "Port 2222" >> /etc/ssh/sshd_config && \
    echo "    Port 2222" >> /etc/ssh/ssh_config && \
    echo "    StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    echo "    UserKnownHostsFile /dev/null" >> /etc/ssh/ssh_config
```
- **`mkdir -p /run/sshd`**: Tạo thư mục runtime cần thiết để tiến trình `sshd` có thể khởi động không báo lỗi.
- **`su - ${HADOOP_USER} -c "..."`**: Đổi sang quyền của user `hadoopanh24162006` để:
  - `ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa`: Tự tạo cặp khóa SSH (Private Key `id_rsa` và Public Key `id_rsa.pub`) không có passphrase (`-P ''`).
  - `cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys`: Thêm public key vào danh sách tin cậy để máy tính có thể tự SSH vào chính nó (localhost) không cần mật khẩu.
  - `chmod 600 ~/.ssh/authorized_keys`: Đặt quyền truy cập an toàn cho file (chỉ chủ sở hữu mới đọc/ghi được), nếu quyền lỏng lẻo OpenSSH sẽ từ chối nhận key.
- **`echo "Port 2222" >> /etc/ssh/sshd_config`**: Yêu cầu dịch vụ SSH Server bên trong container lắng nghe ở **cổng 2222** thay vì cổng 22 chuẩn. Điều này tránh xung đột với dịch vụ SSH hoặc hệ điều hành Windows ở máy thật.
- **`echo "    Port 2222" >> /etc/ssh/ssh_config`**: Cấu hình SSH Client mặc định sẽ kết nối qua cổng 2222 khi thực hiện lệnh `ssh <hostname>`.
- **`StrictHostKeyChecking no` và `UserKnownHostsFile /dev/null`**: Tắt hộp thoại xác nhận vân tay máy chủ (`Are you sure you want to continue connecting (yes/no)?`) giúp Hadoop tự động SSH xuyên suốt các node mà không bị treo chờ xác nhận.

```dockerfile
# Bung bộ cài Offline từ Host vào Container
ADD --chown=${HADOOP_USER}:${HADOOP_USER} offline_pkgs/hadoop-3.5.0.tar.gz /home/${HADOOP_USER}/
ADD --chown=${HADOOP_USER}:${HADOOP_USER} offline_pkgs/spark-4.2.0-bin-hadoop3.gz /home/${HADOOP_USER}/
ADD --chown=${HADOOP_USER}:${HADOOP_USER} offline_pkgs/apache-hive-4.1.0-bin.tar.gz /home/${HADOOP_USER}/
```
- Lệnh **`ADD`** trong Dockerfile có tính năng đặc biệt: tự động giải nén các file `.tar.gz` hoặc `.tgz` từ máy Host trực tiếp vào thư mục chỉ định trong container.
- Cờ `--chown=${HADOOP_USER}:${HADOOP_USER}`: Đảm bảo toàn bộ file sau khi giải nén thuộc quyền sở hữu của user `hadoopanh24162006`, tránh lỗi quyền `Permission denied`.

```dockerfile
WORKDIR /home/${HADOOP_USER}
RUN mv hadoop-3.5.0 hadoop && \
    mv spark-4.2.0-bin-hadoop3 spark && \
    mv apache-hive-4.1.0-bin hive && \
    mkdir -p hadoop/hdfs/namenode hadoop/hdfs/datanode hadoop/nm-local-dir hadoop/logs && \
    chown -R ${HADOOP_USER}:${HADOOP_USER} /home/${HADOOP_USER}
```
- **`WORKDIR /home/${HADOOP_USER}`**: Đặt thư mục làm việc hiện tại vào thư mục home của user.
- **`mv ...`**: Đổi tên các thư mục vừa bung ra thành các tên ngắn gọn, chuẩn mực: `hadoop`, `spark`, `hive`.
- **`mkdir -p ...`**: Tạo sẵn các thư mục chứa dữ liệu của HDFS NameNode, DataNode, thư mục tạm của NodeManager và thư mục lưu trữ log.
- **`chown -R ...`**: Xác lập lại quyền sở hữu một lần nữa cho chắc chắn.

```dockerfile
# Khai báo biến môi trường hệ thống
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV HADOOP_HOME=/home/${HADOOP_USER}/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV HADOOP_MAPRED_HOME=${HADOOP_HOME}
ENV HADOOP_COMMON_HOME=${HADOOP_HOME}
ENV HADOOP_HDFS_HOME=${HADOOP_HOME}
ENV HADOOP_YARN_HOME=${HADOOP_HOME}
ENV HIVE_HOME=/home/${HADOOP_USER}/hive
ENV SPARK_HOME=/home/${HADOOP_USER}/spark
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV PATH=$PATH:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${HIVE_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
```
- Định nghĩa toàn bộ các biến môi trường chuẩn của hệ sinh thái Apache Big Data.
- `PATH`: Cho phép bạn đứng ở bất kỳ thư mục nào cũng có thể gõ trực tiếp các lệnh như `hdfs`, `hadoop`, `yarn`, `spark-submit`, `pyspark`, `hive`, `beeline`, `start-all.sh`, `stop-all.sh`.
- `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON`: Chỉ định PySpark sử dụng đúng trình thông dịch Python 3 của hệ thống.

```dockerfile
# Tập trung biến môi trường vào /etc/profile.d/hadoop.sh và nạp cho cả interactive / non-interactive shell
RUN printf '%s\n' \
    "export JAVA_HOME=${JAVA_HOME}" \
    "export HADOOP_HOME=${HADOOP_HOME}" \
    "export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}" \
    "export HADOOP_MAPRED_HOME=${HADOOP_MAPRED_HOME}" \
    "export HADOOP_COMMON_HOME=${HADOOP_COMMON_HOME}" \
    "export HADOOP_HDFS_HOME=${HADOOP_HDFS_HOME}" \
    "export HADOOP_YARN_HOME=${HADOOP_YARN_HOME}" \
    "export HIVE_HOME=${HIVE_HOME}" \
    "export SPARK_HOME=${SPARK_HOME}" \
    "export PYSPARK_PYTHON=/usr/bin/python3" \
    "export PYSPARK_DRIVER_PYTHON=/usr/bin/python3" \
    "export CLASSPATH=\$CLASSPATH:\$HADOOP_HOME/lib/*:\$HIVE_HOME/lib/*" \
    "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HIVE_HOME/bin:\$SPARK_HOME/bin:\$SPARK_HOME/sbin" \
    > /etc/profile.d/hadoop.sh && \
    chmod +x /etc/profile.d/hadoop.sh && \
    echo "source /etc/profile.d/hadoop.sh" >> /home/${HADOOP_USER}/.bashrc && \
    echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_CONF_DIR}/hadoop-env.sh && \
    echo "export HADOOP_HOME=${HADOOP_HOME}" >> ${HIVE_HOME}/conf/hive-env.sh
```
- Khắc phục đặc tính của Linux: Khi đăng nhập qua SSH dạng non-interactive (chẳng hạn khi Master gọi `ssh hoc-slave1 "hdfs --daemon start datanode"`), Linux không tự load file `~/.bashrc`.
- Bằng cách ghi vào `/etc/profile.d/hadoop.sh`, `hadoop-env.sh`, và `hive-env.sh`, toàn bộ các biến môi trường luôn sẵn sàng ở mọi tình huống chạy lệnh.

```dockerfile
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
```
- Sao chép script khởi động `entrypoint.sh` vào thư mục nhị phân hệ thống `/usr/local/bin/` và gán quyền thực thi (`chmod +x`).
- `ENTRYPOINT`: Khai báo tiến trình gốc khi container khởi động.

---

## 3. Giải thích chi tiết từng dòng lệnh trong `docker-compose.yml`

File `docker-compose.yml` định nghĩa thông số vận hành của Container trên từng máy laptop của mỗi thành viên.

```yaml
services:
  hadoop-node:
    build: .
    image: bigdata-ecosystem:latest
```
- `hadoop-node`: Tên định danh service trong Docker Compose.
- `build: .`: Nếu image chưa có hoặc có cờ `--build`, Docker Compose sẽ tìm `Dockerfile` ở thư mục hiện tại để xây dựng.
- `image: bigdata-ecosystem:latest`: Tên và tag của Docker Image được sinh ra.

```yaml
    hostname: ${NODE_HOSTNAME:-anh24162006-master}
    container_name: ${NODE_HOSTNAME:-anh24162006-master}
```
- `hostname`: Tên máy hiển thị bên trong container (ví dụ `hadoopanh24162006@anh24162006-master:~$`). Tên này được đọc từ biến `NODE_HOSTNAME` trong file `.env`. Nếu không có file `.env`, nó nhận giá trị mặc định là `anh24162006-master`.
- `container_name`: Tên của container khi hiển thị trong danh sách `docker ps`.

```yaml
    entrypoint: ["/bin/bash", "-c", "tr -d '\\r' < /usr/local/bin/entrypoint.sh | /bin/bash"]
```
- **Kỹ thuật quan trọng xử lý lỗi Windows CRLF**: Trên hệ điều hành Windows, các file văn bản thường có ký tự xuống dòng `\r\n` (CRLF). Khi mount vào Linux, ký tự `\r` sẽ gây lỗi cú pháp `\r: command not found`.
- Lệnh này sử dụng `tr -d '\r'` để lọc bỏ toàn bộ ký tự `\r` trước khi đưa vào Bash thực thi, đảm bảo chạy an toàn tuyệt đối trên mọi máy Windows.

```yaml
    environment:
      - NODE_ROLE=${NODE_ROLE:-master}
```
- Truyền biến môi trường `NODE_ROLE` vào container (nhận giá trị `master` hoặc `slave` từ file `.env`). Dựa vào biến này, `entrypoint.sh` sẽ biết container này là Master (cần format NameNode) hay Slave.

```yaml
    extra_hosts:
      - "anh24162006-master:100.105.99.31"
      - "hoc-slave1:100.123.200.77"
      - "nguyen-slave2:100.106.204.115"
      - "anh24133004-slave3:100.112.244.83"
```
- **Phân giải tên miền qua Tailscale Mesh VPN**:
- Docker tự động ghi 4 dòng này vào file `/etc/hosts` bên trong container.
- Giúp các container phân giải trực tiếp tên máy của các thành viên sang địa chỉ IP Tailscale thật mà không cần phải dựng một máy chủ DNS riêng biệt.

```yaml
    ports:
      - "2222:2222"     # SSH giữa các node
      - "9000:9000"     # HDFS NameNode RPC
      - "9870:9870"     # HDFS NameNode Web UI
      - "9864:9864"     # HDFS DataNode Web UI
      - "9866:9866"     # HDFS DataNode Data Transfer
      - "9867:9867"     # HDFS DataNode IPC
      - "8030:8030"     # YARN ResourceManager Scheduler
      - "8031:8031"     # YARN ResourceManager Tracker / MRAppMaster
      - "8032:8032"     # YARN ResourceManager Address
      - "8033:8033"     # YARN ResourceManager Admin / NodeManager
      - "8088:8088"     # YARN ResourceManager Web UI
      - "8042:8042"     # YARN NodeManager Web UI
      - "10020:10020"   # MapReduce JobHistory RPC / Slave 2 xferPort
      - "19888:19888"   # MapReduce JobHistory Web UI / Slave 3 xferPort
      - "10000:10000"   # HiveServer2 JDBC
      - "7077:7077"     # Spark Standalone Master
      - "8080:8080"     # Spark Master Web UI
      - "4040:4040"     # Spark Application UI / MapReduce Shuffle Handler
```
- Định dạng cú pháp: `"Host_Port:Container_Port"`.
- Mở và chuyển tiếp toàn bộ các cổng mạng cần thiết từ mạng Tailscale của máy Host vào bên trong container. Nhờ danh sách cổng này, máy Master và 3 máy Slave có thể truyền nhận dữ liệu HDFS khối lớn và điều phối tính toán MapReduce/Spark xuyên suốt qua Internet.

```yaml
    volumes:
      - ./entrypoint.sh:/usr/local/bin/entrypoint.sh
      - ./config/core-site.xml:/home/hadoopanh24162006/hadoop/etc/hadoop/core-site.xml
      - ./config/hdfs-site.xml:/home/hadoopanh24162006/hadoop/etc/hadoop/hdfs-site.xml
      - ./config/yarn-site.xml:/home/hadoopanh24162006/hadoop/etc/hadoop/yarn-site.xml
      - ./config/mapred-site.xml:/home/hadoopanh24162006/hadoop/etc/hadoop/mapred-site.xml
      - ./config/workers:/home/hadoopanh24162006/hadoop/etc/hadoop/workers
      - ./config/workers:/home/hadoopanh24162006/spark/conf/workers
      - ./config/hive-site.xml:/home/hadoopanh24162006/hive/conf/hive-site.xml
      - ./test_bigdata:/home/hadoopanh24162006/test_bigdata
      - hadoop_data:/home/hadoopanh24162006/hadoop/hdfs
    restart: unless-stopped

volumes:
  hadoop_data:
```
- **Bind Mounts (`./config/...`)**: Đồng bộ trực tiếp các file cấu hình XML và mã nguồn kiểm thử từ thư mục trên máy Host vào trong container. Khi bạn sửa file cấu hình ở ngoài máy tính (VS Code), bên trong container nhận ngay lập tức mà không cần phải gõ lệnh build lại image.
- **Named Volume (`hadoop_data`)**: Vùng nhớ lưu trữ dữ liệu HDFS phân tán. Vùng nhớ này được Docker quản lý riêng biệt, đảm bảo khi container bị tắt hoặc restart thì dữ liệu HDFS không bị mất.
- **`restart: unless-stopped`**: Nếu container bị tắt đột ngột hoặc máy tính khởi động lại, Docker Engine sẽ tự động bật lại container này.

---

## 4. Giải thích chi tiết từng dòng lệnh trong `entrypoint.sh`

Kịch bản `entrypoint.sh` được tự động chạy mỗi khi container khởi động (Container Lifecycle Startup Script).

```bash
#!/bin/bash
set -e
```
- `#!/bin/bash`: Chỉ thị script chạy bằng trình thông dịch Bourne-Again Shell.
- `set -e`: Nếu bất kỳ câu lệnh nào trong script trả về mã lỗi khác 0, script sẽ dừng ngay lập tức để ngăn ngừa lỗi dây chuyền.

```bash
# Đồng bộ mật khẩu user hadoopanh24162006
echo "hadoopanh24162006:hadoopanh24162006" | chpasswd
```
- Đảm bảo mật khẩu của user luôn khớp chính xác là `hadoopanh24162006`.

```bash
# Cấu hình SSH chạy trên port 2222 (tránh xung đột port 22 của máy host và tăng bảo mật)
grep -q "^Port 2222" /etc/ssh/sshd_config || echo "Port 2222" >> /etc/ssh/sshd_config
grep -q "Port 2222" /etc/ssh/ssh_config || echo "    Port 2222" >> /etc/ssh/ssh_config
grep -q "HADOOP_SSH_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh || \
    echo 'export HADOOP_SSH_OPTS="-p 2222"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh
```
- Kiểm tra xem file cấu hình SSH đã có thiết lập cổng 2222 chưa (`grep -q`). Nếu chưa có thì ghi thêm vào.
- Đặc biệt, gán biến `HADOOP_SSH_OPTS="-p 2222"` vào `hadoop-env.sh`. Khi máy Master chạy `start-all.sh`, các tiến trình con sẽ tự động dùng port 2222 để gọi sang các máy Slave.

```bash
# Tránh trùng cặp (NAT_IP:xferPort) khi 3 DataNode đi qua Docker Bridge NAT về Master
if [ "$(hostname)" = "nguyen-slave2" ]; then
    grep -q "HDFS_DATANODE_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh || \
        echo 'export HDFS_DATANODE_OPTS="-Ddfs.datanode.address=0.0.0.0:10020"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh
elif [ "$(hostname)" = "anh24133004-slave3" ]; then
    grep -q "HDFS_DATANODE_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh || \
        echo 'export HDFS_DATANODE_OPTS="-Ddfs.datanode.address=0.0.0.0:19888"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh
fi
```
- **Kỹ thuật then chốt giải quyết xung đột Docker NAT**:
- Khi 3 máy Slave đi qua cầu nối mạng Docker Desktop để đăng ký với NameNode trên Master, NameNode nhìn thấy IP nguồn của cả 3 máy đều là IP Gateway `172.18.0.1`.
- Nếu cả 3 máy cùng mở cổng truyền dữ liệu mặc định là `9866`, NameNode sẽ thấy 3 định danh giống nhau là `172.18.0.1:9866` và ghi đè lẫn nhau (dẫn đến chỉ nhận 1 DataNode duy nhất).
- Đoạn mã này gán cho mỗi Slave một cổng truyền dữ liệu (`xferPort`) khác nhau:
  - `hoc-slave1`: Giữ cổng mặc định `9866`.
  - `nguyen-slave2`: Đổi sang cổng `10020`.
  - `anh24133004-slave3`: Đổi sang cổng `19888`.
- Nhờ vậy, NameNode phân biệt được rõ ràng cả 3 DataNode độc lập và nhận đủ dung lượng lưu trữ phân tán.

```bash
# Khởi động SSH daemon
service ssh start
```
- Khởi động dịch vụ OpenSSH Server để container sẵn sàng nhận các kết nối điều khiển từ xa.

```bash
# Đảm bảo quyền sở hữu thư mục dữ liệu HDFS khi mount volume
chown -R hadoopanh24162006:hadoopanh24162006 /home/hadoopanh24162006/hadoop/hdfs
```
- Khi Docker mount volume `hadoop_data` vào container, thư mục này có thể bị đổi quyền sang `root`. Lệnh `chown` trao trả lại toàn quyền sở hữu cho user `hadoopanh24162006` để Hadoop có thể đọc/ghi khối dữ liệu bình thường.

```bash
if [ "$NODE_ROLE" = "master" ]; then
    # Chỉ format NameNode nếu thư mục current chưa tồn tại (tránh lỗi Incompatible clusterIDs)
    if [ ! -d "/home/hadoopanh24162006/hadoop/hdfs/namenode/current" ]; then
        echo "=== Formatting NameNode for the first time ==="
        su - hadoopanh24162006 -c "/home/hadoopanh24162006/hadoop/bin/hdfs namenode -format -force"
    fi
fi
```
- **Cơ chế an toàn tránh mất dữ liệu ClusterID**:
- Nếu là máy Master (`NODE_ROLE=master`), script kiểm tra xem thư mục `namenode/current` đã có chưa.
- Chỉ khi khởi động cụm lần đầu tiên (thư mục chưa có), nó mới chạy lệnh `hdfs namenode -format -force`.
- Nếu cụm đã chạy từ trước và container được khởi động lại, nó **bỏ qua** bước format. Điều này ngăn chặn việc ClusterID của NameNode bị đổi mới làm lệch pha với các DataNode trên máy Slave (tránh lỗi nghiêm trọng `Incompatible clusterIDs`).

```bash
# Giữ container luôn chạy để người dùng thao tác và chụp ảnh màn hình báo cáo
tail -f /dev/null
```
- Theo thiết kế của Docker, một container sẽ tự động tắt ngay lập tức nếu tiến trình khởi động kết thúc.
- `tail -f /dev/null` là một tiến trình chạy vô tận, tiêu tốn gần như 0% CPU và RAM. Nó giữ cho container luôn ở trạng thái `RUNNING` để bạn có thể mở terminal tương tác, chạy các lệnh MapReduce, Hive, PySpark và kiểm tra cụm bất cứ lúc nào.

---

## 5. Tóm tắt kiến trúc Container tạo ra trên mỗi Laptop

Khi bạn hoàn tất lệnh `docker compose up -d --build`:
1. Bạn có một môi trường Linux Ubuntu 24.04 hoàn chỉnh, độc lập với Windows.
2. Các phần mềm Hadoop 3.5.0, Spark 4.2.0, Hive 4.1.0, Java 17 đã được cài đặt và cấu hình đường dẫn chuẩn xác.
3. Không gian mạng của Container được mở 18 cổng giao tiếp then chốt kết nối thông qua Tailscale VPN sang 3 laptop của các bạn còn lại trong nhóm.
4. Dữ liệu HDFS được bảo toàn an toàn trên Docker Volume.

