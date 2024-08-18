
#!/bin/bash
# Perform the backup
hdfs dfs -copyToLocal -f $HDFS_PATH $LOCAL_BACKUP_DIR