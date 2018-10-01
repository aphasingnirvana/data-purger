# data-purger
FINBKP Data Purging tool to remove HDFS/Hive Tables and keep for last-few days as passed by user
Command:
SPARK_MAJOR_VERSION=2 spark-submit --class purge --master yarn --deploy-mode client --keytab <path to keytab file> --principal <keytab principal> --num-executors 2 --executor-cores 2 --executor-memory 4g --driver-memory 4g --driver-cores 4 --jars /home/fdlhdpetl/jars/postgresql-42.1.4.jar --files /usr/hdp/current/spark2-client/conf/hive-site.xml,datapurge.properties --name DATA-PURGE --conf spark.executor.extraClassPath=/home/fdlhdpetl/jars/postgresql-42.1.4.jar --driver-class-path /home/fdlhdpetl/jars/postgresql-42.1.4.jar  --conf spark.eventLog.compress=true --conf spark.eventLog.enabled=true datapurger_2.11-dev.jar --keep-days <number of days to keep data for>
