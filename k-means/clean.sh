export JAVA_HOME=/usr/lib/jvm/default
stop-yarn.sh
stop-dfs.sh
rm -rf /tmp/hadoop*
hdfs namenode -format
start-dfs.sh
hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/willian
hdfs dfs -put worldcitiespop.txt worldcitiespop.txt
start-yarn.sh

