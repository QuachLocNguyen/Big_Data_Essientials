# Báo Cáo Kỹ Thuật: Tối Ưu Quota YARN Capacity Scheduler & Cấp Phát Bộ Nhớ Cho ApplicationMaster (AM)

---

## 1. Vấn Đề Gặp Phải (Problem Statement)

Khi cụm phân tán Hadoop/YARN thực thi các tác vụ MapReduce hoặc Hive (đặc biệt là các truy vấn có nhiều giai đoạn như `Stage-1` gom nhóm và `Stage-2` sắp xếp dữ liệu), ứng dụng rơi vào trạng thái bị nghẽn:

```text
Application-Id: application_1792743013638_0002
State: ACCEPTED
Progress: 0%
Tracking-URL: N/A
AM Host: N/A
Diagnostics: Job yêu cầu 1536 MB RAM cho ApplicationMaster, nhưng giới hạn AM của queue root.default hiện là 1024 MB.
```

Ứng dụng liên tục dừng ở trạng thái `ACCEPTED` mà không chuyển sang `RUNNING`, gây nghẽn toàn bộ hàng đợi tính toán của cụm.

---

## 2. Phân Tích Nguyên Nhân Gốc Rễ (Root Cause Analysis)

### 2.1. Bản chất của YARN Capacity Scheduler
Trong kiến trúc Hadoop YARN, **Capacity Scheduler** là bộ lập lịch tài nguyên đa người dùng mặc định. Để bảo vệ cụm không bị tình trạng **"AM cướp hết tài nguyên"** (nghĩa là toàn bộ RAM bị các tiến trình điều phối ApplicationMaster chiếm sạch, khiến không còn RAM để chạy các Container tính toán Mapper và Reducer thực tế), YARN áp dụng thông số:
$$\text{AM Resource Quota} = \text{Total Cluster RAM} \times \text{maximum-am-resource-percent}$$

### 2.2. Sự mất cân đối trong cụm máy thử nghiệm
* **Tổng tài nguyên cụm:** Cụm gồm 2 Worker Node (`hoc-slave1` và `anh24133004-slave3`), mỗi node cấp phát `yarn.nodemanager.resource.memory-mb = 4096 MB` (4 GB). Tổng RAM YARN quản lý là **8192 MB (8 GB)**.
* **Giá trị mặc định của `maximum-am-resource-percent`:** Thường là `0.1` đến `0.125` (10% - 12.5%).
  $$\text{Quota AM tối đa của queue root.default} = 8192\text{ MB} \times 0.125 = \mathbf{1024\text{ MB RAM}}$$
* **Nhu cầu của MapReduce AM:** Mặc định của Hadoop MapReduce (`yarn.app.mapreduce.am.resource.mb`) là **1536 MB RAM**.
* **Hậu quả:** 
  $$1536\text{ MB (Cần)} > 1024\text{ MB (Quota cấp phép)}$$
  YARN từ chối cấp phát container cho ApplicationMaster, khiến job bị treo vô thời hạn ở trạng thái `ACCEPTED`.

---

## 3. Giải Pháp Kỹ Thuật Đã Áp Dụng (Applied Solution)

Để giải quyết triệt để và an toàn cho hệ thống, nhóm đã kết hợp đồng thời **2 giải pháp kỹ thuật**:

```
                  ┌──────────────────────────────────────────────┐
                  │          Tổng RAM Cụm: 8192 MB (8 GB)         │
                  └──────────────────────┬───────────────────────┘
                                         │
                 ┌───────────────────────┴───────────────────────┐
                 ▼                                               ▼
     【CÁCH 1: Capacity Scheduler】                 【CÁCH 2: MapReduce AM】
  Nâng quota AM: 12.5% ──► 50% (4096 MB)          Hạ RAM yêu cầu: 1536 MB ──► 1024 MB
                 │                                               │
                 └───────────────────────┬───────────────────────┘
                                         ▼
                  ┌──────────────────────────────────────────────┐
                  │  AM (1024 MB) <= Quota (4096 MB)             │
                  │  ==> Job chuyển RUNNING ngay lập tức!        │
                  └──────────────────────────────────────────────┘
```

---

### Cách 1: Nâng Quota Bộ Nhớ Cho AM Trong `capacity-scheduler.xml`

Tạo file cấu hình [config/capacity-scheduler.xml](file:///d:/UTE/BigDataEssentials/Big_Data_Essientials/config/capacity-scheduler.xml) trên máy **Master**:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <!-- Cho phép toàn bộ cụm dành tối đa 50% RAM cho các ApplicationMaster -->
    <property>
        <name>yarn.scheduler.capacity.maximum-am-resource-percent</name>
        <value>0.5</value>
        <description>Tỷ lệ tài nguyên RAM tối đa trong toàn cụm YARN được phép cấp cho các tiến trình điều phối ApplicationMaster (50% = 4096 MB trên cụm 8 GB).</description>
    </property>

    <!-- Áp dụng riêng cho hàng đợi root.default -->
    <property>
        <name>yarn.scheduler.capacity.root.default.maximum-am-resource-percent</name>
        <value>0.5</value>
        <description>Tỷ lệ tài nguyên RAM tối đa cấp cho AM trong hàng đợi mặc định root.default.</description>
    </property>
</configuration>
```

Đồng thời ánh xạ file này vào [docker-compose.yml](file:///d:/UTE/BigDataEssentials/Big_Data_Essientials/docker-compose.yml) tại máy Master:
```yaml
      - ./config/capacity-scheduler.xml:/home/hadoopanh24162006/hadoop/etc/hadoop/capacity-scheduler.xml
```

---

### Cách 2: Tối Ưu RAM Yêu Cầu Của ApplicationMaster Trong `mapred-site.xml`

Trong môi trường máy tính cá nhân thử nghiệm, việc để 1 AM chiếm 1536 MB là lãng phí. Đã tối ưu hóa thông số trong file [config/mapred-site.xml](file:///d:/UTE/BigDataEssentials/Big_Data_Essientials/config/mapred-site.xml):

```xml
    <!-- Hạ RAM yêu cầu của ApplicationMaster từ 1536 MB xuống 1024 MB -->
    <property>
        <name>yarn.app.mapreduce.am.resource.mb</name>
        <value>1024</value>
        <description>Dung lượng RAM cấp phát cho tiến trình điều phối ApplicationMaster (hạ từ 1536 MB xuống 1024 MB để phù hợp với cụm nhỏ 8 GB RAM và không vượt quota Capacity Scheduler).</description>
    </property>

    <!-- Cấu hình vùng nhớ Heap cho JVM của ApplicationMaster -->
    <property>
        <name>yarn.app.mapreduce.am.command-opts</name>
        <value>-Xmx820m</value>
        <description>Giới hạn bộ nhớ Heap JVM của ApplicationMaster (820 MB = xấp xỉ 80% của 1024 MB RAM container, chừa 20% cho vùng nhớ Metaspace/Off-heap của JVM).</description>
    </property>
```

---

## 4. Hiệu Quả Đạt Được

1. **Khắc phục triệt để lỗi treo `ACCEPTED`:**
   * Quota AM của queue `root.default` tăng từ `1024 MB` lên `4096 MB` (50% của 8 GB).
   * Yêu cầu RAM của mỗi AM giảm từ `1536 MB` xuống `1024 MB`.
   * Cụm hiện tại có khả năng chạy đồng thời lên tới **4 ApplicationMaster cùng lúc** ($4 \times 1024\text{ MB} = 4096\text{ MB}$) mà không bao giờ bị nghẽn queue.
2. **Tiết kiệm tài nguyên cho tác vụ tính toán:**
   * Mỗi AM tiết kiệm được `512 MB RAM`, nhường lại không gian cho các container Mapper và Reducer thực thi tác vụ xử lý dữ liệu lớn.
3. **Linh hoạt khi áp dụng:**
   * Cấu hình Capacity Scheduler có thể nạp lại tức thì vào ResourceManager đang chạy bằng lệnh:
     ```bash
     yarn rmadmin -refreshQueues
     ```
   * Không làm gián đoạn các dịch vụ NameNode, DataNode hay HiveServer2 đang hoạt động.

