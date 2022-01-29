name := "filtered-stream"

version := "0.1"

scalaVersion := "2.12.12"

scalacOptions += "-target:jvm-1.8"

val sparkVersion = "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

libraryDependencies += "com.atilika.kuromoji" % "kuromoji-jumandic" % "0.9.0"

libraryDependencies += "net.sourceforge.argparse4j" % "argparse4j" % "0.9.0"

