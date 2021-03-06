Utility tool which allows you to delete tables/files from HADOOP ecosystem older than certain number of days

Command to run:
SPARK_MAJOR_VERSION=2 spark-submit --class purge --master yarn --deploy-mode client --num-executors 1 --executor-cores 1 --executor-memory 4g --driver-memory 4g --driver-cores 1 --files <path to hive-site.xml> --name DATA-PURGE datapurger_2.11-1.jar --keep-days <**past number of days to keep data for**> --hdfs-path <**HDFS path of the DB on your cluster**> --database <**database name**>

**Example:** With the below command we are deleting all tables which are **older than 7 days** located in **/apps/hive/warehouse/sales.db/** under the **sales** database

SPARK_MAJOR_VERSION=2 spark-submit --class purge --master yarn --deploy-mode client --num-executors 1 --executor-cores 1 --executor-memory 4g --driver-memory 4g --driver-cores 1 --files /usr/hdp/current/spark2-client/conf/hive-site.xml --name DATA-PURGE datapurger_2.11-1.jar --keep-days 7 --hdfs-path /apps/hive/warehouse/sales.db/ --database sales

**CAUTION:**
The code also allows another variation which should be used with caution, without passing the --database field, which just deletes files from the --hdfs-path based on the --keep-days. This does not clear the logical layer information which may be present over the files (table structure, would be stored in metastore as it is). **Use this version of the command only if you're sure about what you're doing.**

**Example:** With the below command we are deleting all files in **/apps/hive/warehouse/sales.db/** which are **older than 7 days** without removing the tables which may be pointed to this HDFS location

SPARK_MAJOR_VERSION=2 spark-submit --class purge --master yarn --deploy-mode client --num-executors 1 --executor-cores 1 --executor-memory 4g --driver-memory 4g --driver-cores 1 --files /usr/hdp/current/spark2-client/conf/hive-site.xml --name DATA-PURGE datapurger_2.11-1.jar --keep-days 7 --hdfs-path /apps/hive/warehouse/sales.db/


