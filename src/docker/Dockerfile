# Hadoop 3.4.1 + Spark 3.5.x — Java 8 (khớp Lab1: hadoop-test.jar cần Java 8).
# Kế thừa image Lab1, thêm Spark. KHÔNG đổi version Hadoop/Java/Scala.
FROM eclipse-temurin:8-jdk-jammy

ENV HADOOP_VERSION=3.4.1 \
    SPARK_VERSION=3.5.3 \
    HADOOP_HOME=/opt/hadoop \
    SPARK_HOME=/opt/spark \
    HDFS_NAMENODE_USER=root HDFS_DATANODE_USER=root HDFS_SECONDARYNAMENODE_USER=root \
    YARN_RESOURCEMANAGER_USER=root YARN_NODEMANAGER_USER=root \
    JAVA_HOME=/opt/java/openjdk
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin

RUN apt-get update && \
    apt-get install -y --no-install-recommends openssh-server openssh-client rsync curl net-tools iproute2 sudo scala && \
    rm -rf /var/lib/apt/lists/*

# Hadoop
RUN curl -fSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz

# Spark (prebuilt bin-hadoop3, Scala 2.12) — dùng chung Hadoop runtime của cluster
RUN curl -fSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# SSH không mật khẩu (Hadoop start scripts cần)
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh && \
    ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 600 /root/.ssh/authorized_keys && \
    echo "StrictHostKeyChecking no" >> /etc/ssh/ssh_config && \
    sed -i 's/#\?PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config

RUN echo "export JAVA_HOME=${JAVA_HOME}" >> ${HADOOP_HOME}/etc/hadoop/hadoop-env.sh
# Spark trỏ HDFS + YARN của cluster này
RUN echo "export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop" >> ${SPARK_HOME}/conf/spark-env.sh

COPY config/core-site.xml ${HADOOP_HOME}/etc/hadoop/core-site.xml
COPY config/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
COPY config/mapred-site.xml ${HADOOP_HOME}/etc/hadoop/mapred-site.xml
COPY config/yarn-site.xml ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 9000 9870 8088 9864 4040
ENTRYPOINT ["/entrypoint.sh"]
