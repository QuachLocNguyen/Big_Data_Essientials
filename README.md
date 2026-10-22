# Hệ Sinh Thái Big Data Phân Tán: Hadoop 3.5.0, Hive 4.1.0 & Spark 4.2.0

Hướng dẫn toàn diện từ A–Z giúp bất kỳ thành viên nào khi `git clone` dự án về đều có thể thiết lập cụm Docker đa máy qua mạng VPN Tailscale, cấu hình SSH không mật khẩu, khởi động cụm **Hadoop (HDFS + YARN + MapReduce)**, xây dựng **Data Warehouse trên Apache Hive (kết nối Beeline & DBeaver)** và chạy các bài toán **PySpark MapReduce**.

> **Tài liệu chi tiết về thiết kế Data Warehouse & xử lý sự cố chuyên sâu:** Xem thêm tại [docs/setup_dw.md](docs/setup_dw.md).

---

## Mục Lục
1. [Kiến Trúc Cụm & Phân Công Thành Viên](#1-kiến-trúc-cụm--phân-công-thành-viên)
2. [Bước 1: Chuẩn Bị Mạng Tailscale & Ánh Xạ Hostname](#2-bước-1-chuẩn-bị-mạng-tailscale--ánh-xạ-hostname)
3. [Bước 2: Chuẩn Bị Bộ Cài Offline & Cấu Hình `.env`](#3-bước-2-chuẩn-bị-bộ-cài-offline--cấu-hình-env)
4. [Bước 3: Khởi Tạo Docker Container & Trao Đổi SSH Key](#4-bước-3-khởi-tạo-docker-container--trao-đổi-ssh-key)
5. [Bước 4: Khởi Động Cụm Hadoop (HDFS, YARN, MapReduce)](#5-bước-4-khởi-động-cụm-hadoop-hdfs-yarn-mapreduce)
6. [Bước 5: Thực Hành Apache Hive Data Warehouse & DBeaver](#6-bước-5-thực-hành-apache-hive-data-warehouse--dbeaver)
7. [Bước 6: Thực Hành Apache Spark & 4 Bài Toán PySpark MapReduce](#7-bước-6-thực-hành-apache-spark--4-bài-toán-pyspark-mapreduce)
8. [Bước 7: Danh Sách Đường Dẫn Giao Diện Web UI (Theo Hostname)](#8-bước-7-danh-sách-đường-dẫn-giao-diện-web-ui-theo-hostname)
9. [Bước 8: Dừng Hệ Thống & Xử Lý Sự Cố Nhanh](#9-bước-8-dừng-hệ-thống--xử-lý-sự-cố-nhanh)

---

## 1. Kiến Trúc Cụm & Phân Công Thành Viên

Cụm gồm **4 Node** chạy trên 4 máy vật lý kết nối ngang hàng (P2P) qua mạng VPN Tailscale, sử dụng chung tài khoản hệ thống `hadoopanh24162006` (mật khẩu: `hadoopanh24162006`) và cổng SSH nội bộ `2222`:

| STT | Hostname | Vai trò (`NODE_ROLE`) | Thành viên phụ trách | IP Tailscale | Dịch vụ đảm nhiệm |
| :-: | :--- | :---: | :--- | :---: | :--- |
| 1 | `anh24162006-master` | `master` | Lê Đặng Hoàng Anh (24162006) | `100.105.99.31` | NameNode, SecondaryNameNode, ResourceManager, JobHistoryServer, HiveServer2, Spark Master |
| 2 | `hoc-slave1` | `slave` | Nguyễn Đức Học (24162039) | `100.123.200.77` | DataNode (port `9866`), NodeManager, Spark Worker |
| 3 | `nguyen-slave2` | `slave` | Quách Lộc Nguyên (21139037) | `100.106.204.115` | DataNode (port `10020`), NodeManager, Spark Worker |
| 4 | `anh24133004-slave3` | `slave` | Nguyễn Văn Hoàng Anh (24133004) | `100.112.244.83` | DataNode (port `19888`), NodeManager, Spark Worker |

---

## 2. Bước 1: Chuẩn Bị Mạng Tailscale & Ánh Xạ Hostname

### 2.1. Kết nối VPN Tailscale và bật kết nối trực tiếp (Direct P2P)
1. Cài đặt **Tailscale** trên cả 4 máy và đăng nhập vào cùng một mạng Tailnet.
2. Trên các máy chạy **Windows**, mở **PowerShell (Run as Administrator)** và chạy cụm lệnh sau để bật IPv6 và mở cổng UDP `41641` giúp kết nối trực tiếp (`direct`), tránh bị đi vòng qua trạm trung chuyển (`relay`):
   ```powershell
   Enable-NetAdapterBinding -Name "*" -ComponentID ms_tcpip6
   Set-ItemProperty -Path "HKLM:\SYSTEM\CurrentControlSet\Services\Tcpip6\Parameters" -Name "DisabledComponents" -Value 0 -Type DWord -ErrorAction SilentlyContinue
   New-NetFirewallRule -DisplayName "Tailscale Direct UDP 41641" -Direction Inbound -Protocol UDP -LocalPort 41641 -Action Allow -Profile Any -ErrorAction SilentlyContinue
   Restart-Service Tailscale
   ```
3. Kiểm tra trạng thái kết nối giữa các máy:
   ```powershell
   tailscale status
   tailscale ping anh24162006-master
   ```

### 2.2. Khai báo Hostname trên máy thật (Host) để mở Web UI và DBeaver bằng Hostname
Để trình duyệt và DBeaver trên máy thật hiểu được các tên miền `anh24162006-master`, `hoc-slave1`, `nguyen-slave2`, `anh24133004-slave3` (trong container Docker đã tự động có nhờ `extra_hosts` trong `docker-compose.yml`), hãy thêm 4 dòng sau vào file `hosts` của máy thật:

* **Trên Windows** (Mở PowerShell với quyền **Administrator**):
  ```powershell
  Add-Content -Path "$env:SystemRoot\System32\drivers\etc\hosts" -Value "`n100.105.99.31 anh24162006-master`n100.123.200.77 hoc-slave1`n100.106.204.115 nguyen-slave2`n100.112.244.83 anh24133004-slave3"
  ```
* **Trên Linux / macOS**:
  ```bash
  sudo tee -a /etc/hosts <<EOF
  100.105.99.31 anh24162006-master
  100.123.200.77 hoc-slave1
  100.106.204.115 nguyen-slave2
  100.112.244.83 anh24133004-slave3
  EOF
  ```

---

## 3. Bước 2: Chuẩn Bị Bộ Cài Offline & Cấu Hình `.env`

### 3.1. Clone dự án và kiểm tra thư mục `offline_pkgs/`
```bash
git clone <URL_REPO_CUA_NHOM>
cd Big_Data_Essientials
```
Đảm bảo trong thư mục `offline_pkgs/` có đủ 3 file nén bộ cài gốc:
* `offline_pkgs/hadoop-3.5.0.tar.gz`
* `offline_pkgs/apache-hive-4.1.0-bin.tar.gz`
* `offline_pkgs/spark-4.2.0-bin-hadoop3.gz`

### 3.2. Tạo file `.env` tại thư mục gốc của từng máy
Tạo file `.env` nằm cùng cấp với `docker-compose.yml` theo đúng vai trò của từng máy:

* **Tại máy Master (`anh24162006-master`):**
  ```env
  NODE_HOSTNAME=anh24162006-master
  NODE_ROLE=master
  ```
* **Tại máy Slave 1 (`hoc-slave1`):**
  ```env
  NODE_HOSTNAME=hoc-slave1
  NODE_ROLE=slave
  ```
* **Tại máy Slave 2 (`nguyen-slave2`):**
  ```env
  NODE_HOSTNAME=nguyen-slave2
  NODE_ROLE=slave
  ```
* **Tại máy Slave 3 (`anh24133004-slave3`):**
  ```env
  NODE_HOSTNAME=anh24133004-slave3
  NODE_ROLE=slave
  ```

---

## 4. Bước 3: Khởi Tạo Docker Container & Trao Đổi SSH Key

### 4.1. Build và khởi chạy Container (Thực hiện trên cả 4 máy)
```bash
docker compose up -d --build
```
Kiểm tra container đã chạy:
```bash
docker ps
```

### 4.2. Truy cập vào bên trong Container với user `hadoopanh24162006`
* **Trên máy Master:**
  ```bash
  docker exec -it anh24162006-master su - hadoopanh24162006
  ```
* *(Tương tự trên các máy Slave, thay `anh24162006-master` bằng `hoc-slave1`, `nguyen-slave2`, hoặc `anh24133004-slave3`).*

### 4.3. Trao đổi SSH Key từ máy Master sang tất cả các Node (Chạy trên `anh24162006-master`)
Khi build image, mỗi container đã tự sinh cặp khóa RSA tại `~/.ssh/id_rsa.pub`. Từ bên trong container **`anh24162006-master`**, gửi khóa công khai sang chính nó và 3 máy Slave qua cổng `2222` (mật khẩu nhập khi được hỏi là `hadoopanh24162006`):

```bash
# Copy SSH key vào chính máy Master và 3 máy Slave qua cổng 2222
ssh-copy-id -p 2222 -i ~/.ssh/id_rsa.pub hadoopanh24162006@anh24162006-master
ssh-copy-id -p 2222 -i ~/.ssh/id_rsa.pub hadoopanh24162006@hoc-slave1
ssh-copy-id -p 2222 -i ~/.ssh/id_rsa.pub hadoopanh24162006@nguyen-slave2
ssh-copy-id -p 2222 -i ~/.ssh/id_rsa.pub hadoopanh24162006@anh24133004-slave3
```

Kiểm tra đăng nhập SSH không cần mật khẩu từ Master tới các node:
```bash
ssh anh24162006-master hostname
ssh hoc-slave1 hostname
ssh nguyen-slave2 hostname
ssh anh24133004-slave3 hostname
```

---

## 5. Bước 4: Khởi Động Cụm Hadoop (HDFS, YARN, MapReduce)

Thực hiện toàn bộ các lệnh dưới đây từ bên trong container **`anh24162006-master`** (với user `hadoopanh24162006`):

### 5.1. Khởi động dịch vụ trên Master (`NameNode`, `SecondaryNameNode`, `ResourceManager`, `JobHistoryServer`)
```bash
start-all.sh
mapred --daemon start historyserver
jps
```

### 5.2. Khởi động `DataNode` và `NodeManager` trên 3 máy Slave từ xa qua SSH
> **Lưu ý:** Mỗi máy Slave được gán một cổng truyền dữ liệu DataNode (`dfs.datanode.address`) riêng biệt (`9866`, `10020`, `19888`) để tránh xung đột NAT khi đi qua Docker Bridge.

```bash
# Khởi động Slave 1 (hoc-slave1)
ssh hoc-slave1 '~/hadoop/bin/hdfs --daemon stop datanode; ~/hadoop/bin/hdfs --daemon start datanode -Ddfs.datanode.address=0.0.0.0:9866; ~/hadoop/bin/yarn --daemon stop nodemanager; ~/hadoop/bin/yarn --daemon start nodemanager; jps'

# Khởi động Slave 2 (nguyen-slave2)
ssh nguyen-slave2 '~/hadoop/bin/hdfs --daemon stop datanode; ~/hadoop/bin/hdfs --daemon start datanode -Ddfs.datanode.address=0.0.0.0:10020; ~/hadoop/bin/yarn --daemon stop nodemanager; ~/hadoop/bin/yarn --daemon start nodemanager; jps'

# Khởi động Slave 3 (anh24133004-slave3)
ssh anh24133004-slave3 '~/hadoop/bin/hdfs --daemon stop datanode; ~/hadoop/bin/hdfs --daemon start datanode -Ddfs.datanode.address=0.0.0.0:19888; ~/hadoop/bin/yarn --daemon stop nodemanager; ~/hadoop/bin/yarn --daemon start nodemanager; jps'
```

### 5.3. Kiểm tra sức khỏe cụm HDFS & YARN và tắt Safe Mode (nếu có)
```bash
# Đảm bảo NameNode thoát khỏi chế độ Safe Mode (Read-Only)
hdfs dfsadmin -safemode leave

# Kiểm tra danh sách các DataNode đang sống (Live datanodes)
hdfs dfsadmin -report

# Kiểm tra danh sách các NodeManager đang chạy trên YARN
yarn node -list
```

### 5.4. Chạy bài kiểm thử MapReduce WordCount cơ bản trên Hadoop
```bash
bash ~/test_bigdata/test.sh
```

---

## 6. Bước 5: Thực Hành Apache Hive Data Warehouse & DBeaver

### 6.1. Nạp bộ dữ liệu E-commerce (CSV) lên HDFS
Chạy trên terminal của **`anh24162006-master`**:

```bash
# 1. Tạo các thư mục riêng biệt cho từng bảng trên HDFS
hdfs dfs -mkdir -p /ecommerce_dw/staging/customers
hdfs dfs -mkdir -p /ecommerce_dw/staging/products
hdfs dfs -mkdir -p /ecommerce_dw/staging/order_items
hdfs dfs -mkdir -p /ecommerce_dw/staging/raw_orders

# 2. Đẩy 4 file CSV từ thư mục test_bigdata/data/ lên HDFS
hdfs dfs -put -f ~/test_bigdata/data/customer_master.csv /ecommerce_dw/staging/customers/
hdfs dfs -put -f ~/test_bigdata/data/product_catalog.csv /ecommerce_dw/staging/products/
hdfs dfs -put -f ~/test_bigdata/data/order_items.csv /ecommerce_dw/staging/order_items/
hdfs dfs -put -f ~/test_bigdata/data/ecommerce_sales_customer_analytics_150k.csv /ecommerce_dw/staging/raw_orders/

# 3. Kiểm tra dung lượng dữ liệu đã lên HDFS
hdfs dfs -ls -h -R /ecommerce_dw/staging
```

### 6.2. Khởi động dịch vụ `HiveServer2` (Cổng `10000`)
Để vừa dùng được **DBeaver** trên máy thật vừa dùng được **Beeline** trên terminal mà không bị lỗi khóa cơ sở dữ liệu nhúng Derby (`Another instance of Derby may have already booted`), chỉ chạy duy nhất tiến trình **`HiveServer2`**:

```bash
# 1. Đảm bảo HDFS không ở Safe Mode
hdfs dfsadmin -safemode leave

# 2. Tắt các tiến trình Hive cũ bị treo (nếu có) và xóa file khóa .lck của Derby
kill -9 $(jps -ml | grep -E "HiveMetaStore|BeeLine|HiveServer2" | awk '{print $1}') 2>/dev/null
rm -f /home/hadoopanh24162006/hive/metastore_db/*.lck

# 3. Khởi động HiveServer2 chạy ngầm
nohup hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &

# 4. Xem log đợi khoảng 10-15 giây tới khi xuất hiện dòng "Hive Session ID = ..." thì nhấn Ctrl+C
tail -f /tmp/hiveserver2.log
```

### 6.3. Kết nối vào Hive bằng Beeline (Trên Terminal)
Luôn kết nối qua `anh24162006-master:10000` (không dùng `jdbc:hive2://` bỏ trống để tránh tạo Embedded Hive thứ 2 gây khóa Derby):

```bash
beeline -u jdbc:hive2://anh24162006-master:10000/default -n hadoopanh24162006
```
*(Nếu đã ở sẵn trong dấu nhắc `beeline>`, gõ: `!connect jdbc:hive2://anh24162006-master:10000/ecommerce_dw`)*

### 6.4. Kết nối vào Hive bằng DBeaver (Từ máy Windows / Linux Host)
1. Mở **DBeaver** → **New Database Connection** → Chọn **Apache Hive** → **Next**.
2. Nhập thông số kết nối:
   * **Host:** `anh24162006-master`
   * **Port:** `10000`
   * **Database/Schema:** `ecommerce_dw` (hoặc `default`)
   * **Username:** `hadoopanh24162006`
   * **Password:** *(Để trống)*
3. Nhấn **Edit Driver Settings** → Tab **Libraries**:
   * Xóa các thư viện mặc định bị xung đột, nhấn **Add File** và chọn file JAR lấy từ container:
     `/home/hadoopanh24162006/hive/jdbc/hive-jdbc-4.1.0-standalone.jar`
     *(Lệnh copy file driver từ container ra máy thật: `docker cp anh24162006-master:/home/hadoopanh24162006/hive/jdbc/hive-jdbc-4.1.0-standalone.jar .`)*
4. Nhấn **Test Connection** → **Finish**.

### 6.5. Thực thi các kịch bản SQL trên Hive Data Warehouse
Trong giao diện **Beeline** (lưu ý lệnh `!run` trong Beeline **không có dấu `;`** ở cuối) hoặc mở trực tiếp các file `.sql` trên **DBeaver**:

```text
-- 1. Khởi tạo Database ecommerce_dw và 4 bảng Staging (Master)
!run /home/hadoopanh24162006/test_bigdata/hive/01_master_init_table.sql

-- 2. Kiểm tra danh sách bảng và truy vấn thử
USE ecommerce_dw;
SHOW TABLES;
SELECT * FROM staging_customers LIMIT 10;
SELECT * FROM staging_products LIMIT 10;
SELECT order_status, COUNT(*) AS total_orders FROM staging_orders GROUP BY order_status;

-- 3. Chạy các file truy vấn phân tích của các thành viên
!run /home/hadoopanh24162006/test_bigdata/hive/02_slave1_hoc_sales_by_city.sql
!run /home/hadoopanh24162006/test_bigdata/hive/03_slave2_nguyen_sales_by_payment.sql
!run /home/hadoopanh24162006/test_bigdata/hive/04_slave3_anh_sales_by_product.sql
```
*(Chi tiết toàn bộ các câu lệnh tạo bảng Core ORC `dim_customer`, `dim_product`, `fact_orders` Partitioned, `fact_order_items` Bucketed và truy vấn phân tích nâng cao xem tại [docs/setup_dw.md](docs/setup_dw.md)).*

---

## 7. Bước 6: Thực Hành Apache Spark & 4 Bài Toán PySpark MapReduce

### 7.1. Chuẩn bị dữ liệu văn bản trên HDFS cho bài toán WordCount / Mean / Median / StdDev
Chạy trên **`anh24162006-master`**:

```bash
# 1. Sinh dữ liệu mẫu (nếu chưa có file input_text_big.txt)
python3 ~/test_bigdata/generate_data.py

# 2. Tạo thư mục trên HDFS và đẩy file văn bản lên
hdfs dfs -mkdir -p /test_bigdata/input
hdfs dfs -put -f ~/test_bigdata/data/input_text_big.txt /test_bigdata/input/
hdfs dfs -ls -h /test_bigdata/input
```

### 7.2. Khởi động cụm Spark Standalone (Từ máy `anh24162006-master`)
```bash
$SPARK_HOME/sbin/start-all.sh
```

### 7.3. Chạy 4 bài toán PySpark MapReduce của 4 thành viên
Có thể chạy trên **YARN** (`--master yarn`) hoặc **Spark Standalone** (`--master spark://anh24162006-master:7077`):

```bash
# Bài 1 (Master - Lê Đặng Hoàng Anh): Đếm tần suất từ (Word Count)
spark-submit --master yarn ~/test_bigdata/spark/01_master_word_count.py

# Bài 2 (Slave 1 - Nguyễn Đức Học): Tính độ dài từ trung bình trên Customer Review (Word Mean)
spark-submit --master yarn ~/test_bigdata/spark/02_slave1_hoc_word_mean.py

# Bài 3 (Slave 2 - Quách Lộc Nguyên): Tính trung vị độ dài từ (Word Median)
spark-submit --master yarn ~/test_bigdata/spark/03_slave2_nguyen_word_median.py

# Bài 4 (Slave 3 - Nguyễn Văn Hoàng Anh): Tính độ lệch chuẩn độ dài từ (Word Standard Deviation)
spark-submit --master yarn ~/test_bigdata/spark/04_slave3_anh_word_stddev.py
```

---

## 8. Bước 7: Danh Sách Đường Dẫn Giao Diện Web UI (Theo Hostname)

Sau khi đã khai báo hostname vào file `hosts` ở [Bước 1.2](#22-khai-báo-hostname-trên-máy-thật-host-để-mở-web-ui-và-dbeaver-bằng-hostname), bạn có thể truy cập toàn bộ giao diện quản trị và giám sát của cụm bằng **Hostname**:

### Giao diện Quản trị Trung tâm (Trên Master: `anh24162006-master`)
* **HDFS NameNode Web UI (Quản lý File & DataNodes):** [http://anh24162006-master:9870/](http://anh24162006-master:9870/)
* **YARN ResourceManager Web UI (Quản lý Job & Containers):** [http://anh24162006-master:8088/](http://anh24162006-master:8088/)
* **MapReduce JobHistory Server Web UI (Lịch sử & Log chi tiết):** [http://anh24162006-master:19888/](http://anh24162006-master:19888/)
* **HiveServer2 Web UI (Quản lý phiên truy vấn Hive & SQL):** [http://anh24162006-master:10002/](http://anh24162006-master:10002/)
* **Spark Master Web UI (Quản lý cụm Spark Standalone):** [http://anh24162006-master:8080/](http://anh24162006-master:8080/)
* **Spark Application UI (Hiển thị khi đang chạy Job Spark):** [http://anh24162006-master:4040/](http://anh24162006-master:4040/)

### Giao diện Giám sát Từng Máy Slave (DataNode & NodeManager)
* **HDFS DataNode Web UI:**
  * Slave 1 (`hoc-slave1`): [http://hoc-slave1:9864/](http://hoc-slave1:9864/)
  * Slave 2 (`nguyen-slave2`): [http://nguyen-slave2:9864/](http://nguyen-slave2:9864/)
  * Slave 3 (`anh24133004-slave3`): [http://anh24133004-slave3:9864/](http://anh24133004-slave3:9864/)
* **YARN NodeManager Web UI:**
  * Slave 1 (`hoc-slave1`): [http://hoc-slave1:8042/](http://hoc-slave1:8042/)
  * Slave 2 (`nguyen-slave2`): [http://nguyen-slave2:8042/](http://nguyen-slave2:8042/)
  * Slave 3 (`anh24133004-slave3`): [http://anh24133004-slave3:8042/](http://anh24133004-slave3:8042/)

---

## 9. Bước 8: Dừng Hệ Thống & Xử Lý Sự Cố Nhanh

### 9.1. Dừng các dịch vụ an toàn (Chạy trên `anh24162006-master`)
```bash
# 1. Dừng HiveServer2
kill -9 $(jps -ml | grep "HiveServer2" | awk '{print $1}') 2>/dev/null

# 2. Dừng Spark Standalone
$SPARK_HOME/sbin/stop-all.sh

# 3. Dừng JobHistoryServer, YARN và HDFS
mapred --daemon stop historyserver
stop-all.sh
```

### 9.2. Bảng tra cứu xử lý sự cố nhanh (Quick Troubleshooting)

| Triệu chứng lỗi | Nguyên nhân | Câu lệnh khắc phục trên `anh24162006-master` |
| :--- | :--- | :--- |
| `Connection refused` khi kết nối Beeline / DBeaver vào cổng `10000` | HDFS đang ở chế độ `Safe mode is ON` khiến `HiveServer2` không tạo được thư mục đệm trên HDFS và tự tắt. | `hdfs dfsadmin -safemode leave` sau đó chạy lại `nohup hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &` |
| `Another instance of Derby may have already booted the database` | Do gõ lệnh `hive` hoặc `!connect jdbc:hive2://` (thiếu `anh24162006-master:10000`) làm mở tiến trình Derby thứ 2. | Luôn kết nối qua `beeline -u jdbc:hive2://anh24162006-master:10000/ecommerce_dw -n hadoopanh24162006` |
| `!run ...sql; (No such file or directory)` trong Beeline | Có dấu chấm phẩy `;` ở cuối lệnh `!run`. | Xóa dấu `;` ở cuối dòng lệnh `!run`. |
| Job MapReduce chạy rất chậm ở bước `Starting Job` | Có máy Slave đang kết nối Tailscale qua trạm trung chuyển `relay "sin"` thay vì `direct`. | Chạy lệnh bật IPv6 & mở cổng UDP `41641` ở [Bước 1.1](#21-kết-nối-vpn-tailscale-và-bật-kết-nối-trực-tiếp-direct-p2p). |

```text
-- 1. Khởi tạo Database ecommerce_dw và 4 bảng Staging (Master)
!run /home/hadoopanh24162006/test_bigdata/hive/01_master_init_table.sql

-- 2. Kiểm tra danh sách bảng và truy vấn thử
USE ecommerce_dw;
SHOW TABLES;
SELECT * FROM staging_customers LIMIT 10;
SELECT * FROM staging_products LIMIT 10;
SELECT order_status, COUNT(*) AS total_orders FROM staging_orders GROUP BY order_status;

-- 3. Chạy các file truy vấn phân tích của các thành viên
!run /home/hadoopanh24162006/test_bigdata/hive/02_slave1_hoc_sales_by_city.sql
!run /home/hadoopanh24162006/test_bigdata/hive/03_slave2_nguyen_sales_by_payment.sql
!run /home/hadoopanh24162006/test_bigdata/hive/04_slave3_anh_sales_by_product.sql
```
*(Chi tiết toàn bộ các câu lệnh tạo bảng Core ORC `dim_customer`, `dim_product`, `fact_orders` Partitioned, `fact_order_items` Bucketed và truy vấn phân tích nâng cao xem tại [docs/setup_dw.md](docs/setup_dw.md)).*

---

## 7. Bước 6: Thực Hành Apache Spark & 4 Bài Toán PySpark MapReduce

### 7.1. Chuẩn bị dữ liệu văn bản trên HDFS cho bài toán WordCount / Mean / Median / StdDev
Chạy trên **`anh24162006-master`**:

```bash
# 1. Sinh dữ liệu mẫu (nếu chưa có file input_text_big.txt)
python3 ~/test_bigdata/generate_data.py

# 2. Tạo thư mục trên HDFS và đẩy file văn bản lên
hdfs dfs -mkdir -p /test_bigdata/input
hdfs dfs -put -f ~/test_bigdata/data/input_text_big.txt /test_bigdata/input/
hdfs dfs -ls -h /test_bigdata/input
```

### 7.2. Khởi động cụm Spark Standalone (Từ máy `anh24162006-master`)
```bash
$SPARK_HOME/sbin/start-all.sh
```

### 7.3. Chạy 4 bài toán PySpark MapReduce của 4 thành viên
Có thể chạy trên **YARN** (`--master yarn`) hoặc **Spark Standalone** (`--master spark://anh24162006-master:7077`):

```bash
# Bài 1 (Master - Lê Đặng Hoàng Anh): Đếm tần suất từ (Word Count)
spark-submit --master yarn ~/test_bigdata/spark/01_master_word_count.py

# Bài 2 (Slave 1 - Nguyễn Đức Học): Tính độ dài từ trung bình trên Customer Review (Word Mean)
spark-submit --master yarn ~/test_bigdata/spark/02_slave1_hoc_word_mean.py

# Bài 3 (Slave 2 - Quách Lộc Nguyên): Tính trung vị độ dài từ (Word Median)
spark-submit --master yarn ~/test_bigdata/spark/03_slave2_nguyen_word_median.py

# Bài 4 (Slave 3 - Nguyễn Văn Hoàng Anh): Tính độ lệch chuẩn độ dài từ (Word Standard Deviation)
spark-submit --master yarn ~/test_bigdata/spark/04_slave3_anh_word_stddev.py
```

---

## 8. Bước 7: Danh Sách Đường Dẫn Giao Diện Web UI (Theo Hostname)

Sau khi đã khai báo hostname vào file `hosts` ở [Bước 1.2](#22-khai-báo-hostname-trên-máy-thật-host-để-mở-web-ui-và-dbeaver-bằng-hostname), bạn có thể truy cập toàn bộ giao diện quản trị và giám sát của cụm bằng **Hostname**:

### Giao diện Quản trị Trung tâm (Trên Master: `anh24162006-master`)
* **HDFS NameNode Web UI (Quản lý File & DataNodes):** [http://anh24162006-master:9870/](http://anh24162006-master:9870/)
* **YARN ResourceManager Web UI (Quản lý Job & Containers):** [http://anh24162006-master:8088/](http://anh24162006-master:8088/)
* **MapReduce JobHistory Server Web UI (Lịch sử & Log chi tiết):** [http://anh24162006-master:19888/](http://anh24162006-master:19888/)
* **HiveServer2 Web UI (Quản lý phiên truy vấn Hive & SQL):** [http://anh24162006-master:10002/](http://anh24162006-master:10002/)
* **Spark Master Web UI (Quản lý cụm Spark Standalone):** [http://anh24162006-master:8080/](http://anh24162006-master:8080/)
* **Spark Application UI (Hiển thị khi đang chạy Job Spark):** [http://anh24162006-master:4040/](http://anh24162006-master:4040/)

### Giao diện Giám sát Từng Máy Slave (DataNode & NodeManager)
* **HDFS DataNode Web UI:**
  * Slave 1 (`hoc-slave1`): [http://hoc-slave1:9864/](http://hoc-slave1:9864/)
  * Slave 2 (`nguyen-slave2`): [http://nguyen-slave2:9864/](http://nguyen-slave2:9864/)
  * Slave 3 (`anh24133004-slave3`): [http://anh24133004-slave3:9864/](http://anh24133004-slave3:9864/)
* **YARN NodeManager Web UI:**
  * Slave 1 (`hoc-slave1`): [http://hoc-slave1:8042/](http://hoc-slave1:8042/)
  * Slave 2 (`nguyen-slave2`): [http://nguyen-slave2:8042/](http://nguyen-slave2:8042/)
  * Slave 3 (`anh24133004-slave3`): [http://anh24133004-slave3:8042/](http://anh24133004-slave3:8042/)

---

## 9. Bước 8: Dừng Hệ Thống & Xử Lý Sự Cố Nhanh

### 9.1. Dừng các dịch vụ an toàn (Chạy trên `anh24162006-master`)
```bash
# 1. Dừng HiveServer2
kill -9 $(jps -ml | grep "HiveServer2" | awk '{print $1}') 2>/dev/null

# 2. Dừng Spark Standalone
$SPARK_HOME/sbin/stop-all.sh

# 3. Dừng JobHistoryServer, YARN và HDFS
mapred --daemon stop historyserver
stop-all.sh
```

### 9.2. Bảng tra cứu xử lý sự cố nhanh (Quick Troubleshooting)

| Triệu chứng lỗi | Nguyên nhân | Câu lệnh khắc phục trên `anh24162006-master` |
| :--- | :--- | :--- |
| `Connection refused` khi kết nối Beeline / DBeaver vào cổng `10000` | HDFS đang ở chế độ `Safe mode is ON` khiến `HiveServer2` không tạo được thư mục đệm trên HDFS và tự tắt. | `hdfs dfsadmin -safemode leave` sau đó chạy lại `nohup hive --service hiveserver2 > /tmp/hiveserver2.log 2>&1 &` |
| `Another instance of Derby may have already booted the database` | Do gõ lệnh `hive` hoặc `!connect jdbc:hive2://` (thiếu `anh24162006-master:10000`) làm mở tiến trình Derby thứ 2. | Luôn kết nối qua `beeline -u jdbc:hive2://anh24162006-master:10000/ecommerce_dw -n hadoopanh24162006` |
| `!run ...sql; (No such file or directory)` trong Beeline | Có dấu chấm phẩy `;` ở cuối lệnh `!run`. | Xóa dấu `;` ở cuối dòng lệnh `!run`. |
| Job MapReduce chạy rất chậm ở bước `Starting Job` | Có máy Slave đang kết nối Tailscale qua trạm trung chuyển `relay "sin"` thay vì `direct`. | Chạy lệnh bật IPv6 & mở cổng UDP `41641` ở [Bước 1.1](#21-kết-nối-vpn-tailscale-và-bật-kết-nối-trực-tiếp-direct-p2p). |

