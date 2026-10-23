#!/bin/bash
set -e

# 1. Đồng bộ mật khẩu user hadoopanh24162006
echo "hadoopanh24162006:hadoopanh24162006" | chpasswd

# 2. Cấu hình SSH chạy port 2222
grep -q "^Port 2222" /etc/ssh/sshd_config 2>/dev/null || echo "Port 2222" >> /etc/ssh/sshd_config
grep -q "Port 2222" /etc/ssh/ssh_config 2>/dev/null || echo "    Port 2222" >> /etc/ssh/ssh_config
grep -q "HADOOP_SSH_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh 2>/dev/null || \
    echo 'export HADOOP_SSH_OPTS="-p 2222"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh

# 3. Khởi động SSH daemon
service ssh start

# 4. Đảm bảo quyền sở hữu thư mục dữ liệu HDFS khi mount volume
mkdir -p /home/hadoopanh24162006/hadoop/hdfs
chown -R hadoopanh24162006:hadoopanh24162006 /home/hadoopanh24162006/hadoop/hdfs

# 5. Tự động format NameNode lần đầu nếu thư mục metadata chưa tồn tại
if [ "$NODE_ROLE" = "master" ]; then
    if [ ! -d "/home/hadoopanh24162006/hadoop/hdfs/namenode/current" ]; then
        echo "=== Formatting NameNode for the first time ==="
        su - hadoopanh24162006 -c "/home/hadoopanh24162006/hadoop/bin/hdfs namenode -format -force"
    fi
fi

# 6. Giữ container luôn chạy
tail -f /dev/null