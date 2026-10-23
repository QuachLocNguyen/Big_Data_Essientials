#!/bin/bash
set -e

# 1. Làm sạch IP Docker Bridge 172.x trong /etc/hosts (tránh xung đột DNS Tailscale)
if grep -q "172\." /etc/hosts 2>/dev/null; then
    (grep -v "172\." /etc/hosts > /tmp/hosts_clean && cat /tmp/hosts_clean > /etc/hosts && rm -f /tmp/hosts_clean) 2>/dev/null || true
fi

# 2. Đồng bộ mật khẩu user hadoopanh24162006
echo "hadoopanh24162006:hadoopanh24162006" | chpasswd

# 3. Cấu hình SSH chạy port 2222 (tránh xung đột port 22 của máy host và tăng bảo mật)
grep -q "^Port 2222" /etc/ssh/sshd_config 2>/dev/null || echo "Port 2222" >> /etc/ssh/sshd_config
grep -q "Port 2222" /etc/ssh/ssh_config 2>/dev/null || echo "    Port 2222" >> /etc/ssh/ssh_config
grep -q "HADOOP_SSH_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh 2>/dev/null || \
    echo 'export HADOOP_SSH_OPTS="-p 2222"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh

# 4. Tránh trùng cặp (NAT_IP:xferPort) khi 3 DataNode đi qua Docker Bridge NAT về Master
if [ "$(hostname)" = "nguyen-slave2" ]; then
    grep -q "HDFS_DATANODE_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh || \
        echo 'export HDFS_DATANODE_OPTS="-Ddfs.datanode.address=0.0.0.0:10020"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh
elif [ "$(hostname)" = "anh24133004-slave3" ]; then
    grep -q "HDFS_DATANODE_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh || \
        echo 'export HDFS_DATANODE_OPTS="-Ddfs.datanode.address=0.0.0.0:19888"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh
fi

# 5. Khởi động SSH daemon
service ssh start

# 6. Đảm bảo quyền sở hữu thư mục dữ liệu HDFS khi mount volume
mkdir -p /home/hadoopanh24162006/hadoop/hdfs
chown -R hadoopanh24162006:hadoopanh24162006 /home/hadoopanh24162006/hadoop/hdfs

# 7. Tự động format NameNode lần đầu nếu thư mục metadata chưa tồn tại
if [ "$NODE_ROLE" = "master" ]; then
    if [ ! -d "/home/hadoopanh24162006/hadoop/hdfs/namenode/current" ]; then
        echo "=== Formatting NameNode for the first time ==="
        su - hadoopanh24162006 -c "/home/hadoopanh24162006/hadoop/bin/hdfs namenode -format -force"
    fi
fi

# 8. Giữ container luôn chạy để người dùng thao tác và chụp ảnh màn hình báo cáo
tail -f /dev/null