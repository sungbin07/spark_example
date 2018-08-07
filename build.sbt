name := "spark_example"

scalaVersion := "2.11.8"

val sparkVer = "2.2.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVer,
  "org.apache.spark" %% "spark-streaming" % sparkVer,
  "org.apache.spark" %% "spark-sql" % sparkVer,
  "org.apache.spark" %% "spark-mllib" % sparkVer,
  "org.apache.spark" %% "spark-repl" % sparkVer,
  "org.apache.spark" %% "spark-yarn" % sparkVer,
  "org.apache.spark" %% "spark-hive" % sparkVer
)

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

retrieveManaged := true

publishMavenStyle := true
