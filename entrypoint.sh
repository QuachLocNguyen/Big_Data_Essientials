#!/bin/bash
set -e

# Khởi động SSH daemon
service ssh start

# Đảm bảo quyền sở hữu thư mục dữ liệu HDFS khi mount volume
chown -R hadoopanh24162006:hadoopanh24162006 /home/hadoopanh24162006/hadoop/hdfs

if [ "$NODE_ROLE" = "master" ]; then
    # Chỉ format NameNode nếu thư mục current chưa tồn tại (tránh lỗi Incompatible clusterIDs)
    if [ ! -d "/home/hadoopanh24162006/hadoop/hdfs/namenode/current" ]; then
        echo "=== Formatting NameNode for the first time ==="
        su - hadoopanh24162006 -c "/home/hadoopanh24162006/hadoop/bin/hdfs namenode -format -force"
    fi
fi

# Giữ container luôn chạy để người dùng thao tác và chụp ảnh màn hình báo cáo
tail -f /dev/null