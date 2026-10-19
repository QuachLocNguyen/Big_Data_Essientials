#!/bin/bash
set -e

# Đồng bộ mật khẩu user hadoophoanganh24162006
echo "hadoophoanganh24162006:hadoophoanganh24162006" | chpasswd

# Cấu hình SSH chạy trên port 2222 (tránh xung đột port 22 của máy host và tăng bảo mật)
grep -q "^Port 2222" /etc/ssh/sshd_config || echo "Port 2222" >> /etc/ssh/sshd_config
grep -q "Port 2222" /etc/ssh/ssh_config || echo "    Port 2222" >> /etc/ssh/ssh_config
grep -q "HADOOP_SSH_OPTS" /home/hadoophoanganh24162006/hadoop/etc/hadoop/hadoop-env.sh || \
    echo 'export HADOOP_SSH_OPTS="-p 2222"' >> /home/hadoophoanganh24162006/hadoop/etc/hadoop/hadoop-env.sh

# Khởi động SSH daemon
service ssh start

# Đảm bảo quyền sở hữu thư mục dữ liệu HDFS khi mount volume
chown -R hadoophoanganh24162006:hadoophoanganh24162006 /home/hadoophoanganh24162006/hadoop/hdfs

if [ "$NODE_ROLE" = "master" ]; then
    # Chỉ format NameNode nếu thư mục current chưa tồn tại (tránh lỗi Incompatible clusterIDs)
    if [ ! -d "/home/hadoophoanganh24162006/hadoop/hdfs/namenode/current" ]; then
        echo "=== Formatting NameNode for the first time ==="
        su - hadoophoanganh24162006 -c "/home/hadoophoanganh24162006/hadoop/bin/hdfs namenode -format -force"
    fi
fi

# Giữ container luôn chạy để người dùng thao tác và chụp ảnh màn hình báo cáo
tail -f /dev/null