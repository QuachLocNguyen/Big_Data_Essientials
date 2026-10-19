#!/bin/bash
# Kiểm thử cụm Hadoop bằng chương trình MapReduce WordCount mẫu

# 1. Tạo thư mục và file dữ liệu đầu vào ở local
mkdir -p input
echo "Hello World" > input/file1.txt
echo "Hello Hadoop" > input/file2.txt

# 2. Xóa thư mục cũ trên HDFS (nếu có) và đẩy dữ liệu mới lên HDFS
hdfs dfs -rm -r -f input1 output1
hdfs dfs -mkdir -p input1
hdfs dfs -put ./input/* input1

# 3. Chạy chương trình MapReduce WordCount
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar wordcount input1 output1

# 4. In nội dung các file đầu vào trên HDFS
echo -e "\ninput file1.txt:"
hdfs dfs -cat input1/file1.txt

echo -e "\ninput file2.txt:"
hdfs dfs -cat input1/file2.txt

# 5. In kết quả đầu ra của WordCount
echo -e "\nwordcount output:"
hdfs dfs -cat output1/part-r-00000