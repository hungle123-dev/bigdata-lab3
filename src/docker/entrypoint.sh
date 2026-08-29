#!/bin/bash
set -e

# SSH daemon (Hadoop scripts cần)
service ssh start

# Format namenode lần đầu (chỉ khi chưa có)
if [ ! -d /opt/hadoop/data/nn/current ]; then
  echo "[entrypoint] Formatting NameNode..."
  $HADOOP_HOME/bin/hdfs namenode -format -force -nonInteractive
fi

echo "[entrypoint] Starting HDFS + YARN..."
$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh

echo "[entrypoint] Cluster up. jps:"
jps

# Giữ container sống
tail -f $HADOOP_HOME/logs/*namenode*.log 2>/dev/null || tail -f /dev/null
