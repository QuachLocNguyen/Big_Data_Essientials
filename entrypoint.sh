#!/bin/bash
set -e

# Khởi động SSH daemon
service ssh start

# Đảm bảo quyền sở hữu thư mục dữ liệu HDFS khi mount volume
chown -R hadoopnhom:hadoopnhom /home/hadoopnhom/hadoop/hdfs

if [ "$NODE_ROLE" = "master" ]; then
    # Chỉ format NameNode nếu thư mục current chưa tồn tại (tránh lỗi Incompatible clusterIDs)
    if [ ! -d "/home/hadoopnhom/hadoop/hdfs/namenode/current" ]; then
        echo "=== Formatting NameNode for the first time ==="
        su - hadoopnhom -c "hdfs namenode -format -force"
    fi
fi

tail -f /dev/null