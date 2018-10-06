import java.sql.{Connection, DriverManager}

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.AnalysisException

import scala.collection.mutable.{ArrayBuffer, Map}

object purge{

  case class TableNotFoundException(errorMessage: String)  extends Exception(errorMessage)
  val usage = "Usage: <jar> --keep-days <number of days> --hdfs-path <HDFS parent path to delete from> [--database <database to delete from>]"
  var conf = new SparkConf()
    .setAppName("Data-Purger")

  type OptionMap = Map[Symbol, String]

  val log = LogManager.getLogger("Data-Purger")
  Logger.getLogger("org").setLevel(Level.INFO)
  Logger.getLogger("akka").setLevel(Level.INFO)

  def main(args: Array[String]): Unit = {

    val options = optionToMap(Map(), args.toList)
    val keepDays = options.getOrElse('keepdays, "")
    val databaseFilePath = options.getOrElse('hdfspath, "")
    val databaseName = options.getOrElse('database, "")

    if (args.length < 3 || databaseFilePath.isEmpty || keepDays.isEmpty) {
      println(usage)
      log.error("Mandatory parameters not provided")
      println("Mandatory parameters not provided")
      System.exit(1)
    }

    val spark = SparkSession.builder.config(conf).master("yarn").enableHiveSupport().getOrCreate()

    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files: Array[FileStatus] = FileSystem.get(spark.sparkContext.hadoopConfiguration).globStatus(new Path(databaseFilePath + "/*"))


    files.par.foreach(file =>
      if (((System.currentTimeMillis - file.getModificationTime) / 1000 / 60 / 60 / 24) > keepDays.toInt && file.getPath.getName.matches(".*\\d+.*")) {
        println("drop table if exists " + databaseName + "." + file.getPath.getName)

        try {
          if(databaseName.isEmpty) throw new TableNotFoundException("Table not found")
          else{
            spark.sql("drop table if exists " + databaseName + "." + file.getPath.getName)
            println("drop table if exists " + databaseName + "." + file.getPath.getName)
          }
        }
        catch {
          case ae: org.apache.spark.sql.AnalysisException =>{
            println("Table does not exist: " + databaseName + "." + file.getPath.getName)
            log.warn("Table does not exist" + databaseName + "." + file.getPath.getName)
          }
          case tnfe: TableNotFoundException =>{
            println("Table does not exist" + databaseName + "." + file.getPath.getName)
            log.warn("Table does not exist" + databaseName + "." + file.getPath.getName)
          }
          case e: Exception =>{
            println(e.printStackTrace())
            log.error(e.printStackTrace())
            System.exit(1)
          }
        }
        finally {
          if (hdfs.exists(file.getPath)){
            log.info("Removing HDFS file: " + file.getPath)
            println("Removing HDFS file" + file.getPath)
            hdfs.delete(file.getPath)
          }

        }
      }
    )
  }
  //Function to map passed arguments to a Map
  def optionToMap(map : OptionMap, list: List[String]) : OptionMap = {
    list match {
      case Nil => map
      case "--keep-days" :: value :: tail =>
        optionToMap(map ++ Map('keepdays -> value.toString.trim), tail)
      case "--hdfs-path" :: value :: tail =>
        optionToMap(map ++ Map('hdfspath -> value.toString.trim), tail)
      case "--database" :: value :: tail =>
        optionToMap(map ++ Map('database -> value.toString.trim), tail)
      case option :: tail => println("Unknown option " + option)
        sys.exit(1)
    }
  }
}