Utility tool which allows you to delete tables/files from HADOOP ecosystem based on their age in days

Command to run:
SPARK_MAJOR_VERSION=2 spark-submit --class purge --master yarn --deploy-mode client --keytab --principal --num-executors 1 --executor-cores 1 --executor-memory 4g --driver-memory 4g --driver-cores 1 --files /usr/hdp/current/spark2-client/conf/hive-site.xml --name DATA-PURGE datapurger_2.11-dev.jar --keep-days 5


