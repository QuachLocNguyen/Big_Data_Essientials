#!/bin/bash
set -e

# 1. Đợi card tailscale0 sẵn sàng và cập nhật hostname trong /etc/hosts về đúng IP Tailscale (100.x)
for i in {1..20}; do
    TS_IP=$(ifconfig tailscale0 2>/dev/null | grep 'inet ' | awk '{print $2}' | sed 's/addr://')
    if [ -n "$TS_IP" ]; then
        break
    fi
    sleep 1
done

if [ -n "$TS_IP" ]; then
    echo "=== Binding hostname $(hostname) to Tailscale IP: $TS_IP ==="
    (grep -v "$(hostname)" /etc/hosts > /tmp/hosts_clean && \
    echo "$TS_IP $(hostname)" >> /tmp/hosts_clean && \
    cat /tmp/hosts_clean > /etc/hosts && \
    rm -f /tmp/hosts_clean) 2>/dev/null || true
fi

# 2. Đồng bộ mật khẩu user hadoopanh24162006
echo "hadoopanh24162006:hadoopanh24162006" | chpasswd

# 3. Cấu hình SSH chạy port 2222
grep -q "^Port 2222" /etc/ssh/sshd_config 2>/dev/null || echo "Port 2222" >> /etc/ssh/sshd_config
grep -q "Port 2222" /etc/ssh/ssh_config 2>/dev/null || echo "    Port 2222" >> /etc/ssh/ssh_config
grep -q "HADOOP_SSH_OPTS" /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh 2>/dev/null || \
    echo 'export HADOOP_SSH_OPTS="-p 2222"' >> /home/hadoopanh24162006/hadoop/etc/hadoop/hadoop-env.sh

# 4. Khởi động SSH daemon
service ssh start

# 5. Đảm bảo quyền sở hữu thư mục dữ liệu HDFS khi mount volume
mkdir -p /home/hadoopanh24162006/hadoop/hdfs
chown -R hadoopanh24162006:hadoopanh24162006 /home/hadoopanh24162006/hadoop/hdfs

# 6. Tự động format NameNode lần đầu nếu thư mục metadata chưa tồn tại
if [ "$NODE_ROLE" = "master" ]; then
    if [ ! -d "/home/hadoopanh24162006/hadoop/hdfs/namenode/current" ]; then
        echo "=== Formatting NameNode for the first time ==="
        su - hadoopanh24162006 -c "/home/hadoopanh24162006/hadoop/bin/hdfs namenode -format -force"
    fi
fi

# 7. Giữ container luôn chạy
tail -f /dev/null
