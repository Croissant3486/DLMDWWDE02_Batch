FROM ubuntu:20.04

# Install necessary tools
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    wget \
    cron \
    ssh \
    rsync

# Install Hadoop
RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
    && tar -xzvf hadoop-3.3.6.tar.gz -C /opt/ \
    && rm hadoop-3.3.6.tar.gz

ENV HADOOP_HOME=/opt/hadoop-3.3.6
ENV PATH=$PATH:$HADOOP_HOME/bin

# Setup cron job
RUN echo "0 3 * * * /backup/scripts/backup-hdfs.sh" > /etc/cron.d/backup-hdfs-cron
RUN chmod 0644 /etc/cron.d/backup-hdfs-cron
RUN crontab /etc/cron.d/backup-hdfs-cron

CMD ["cron", "-f"]