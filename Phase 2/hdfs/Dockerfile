FROM apache/hadoop:3

# Switch to root user for running the necessary scripts
USER root

# Create a group and sparkuser with a specific UID and home directory
#RUN useradd -ms /bin/bash -u 6969 -g supergroup sparkuser

#CMD ["/bin/bash" "-c" "./hdfs/init-hdfs.sh"]

ENTRYPOINT ["/bin/bash", "-c", "hdfs namenode && /hdfs/init-hdfs.sh"]