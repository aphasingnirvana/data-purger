lazy val buildSettings = Seq(
  name := "DataPurger",
  version := "1",
  scalaVersion := "2.11.9"
)

val app = (project in file(".")).
  settings(buildSettings: _*)


libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0" ,
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0" ,
  "org.apache.spark" % "spark-hive_2.11" % "2.1.0"
)