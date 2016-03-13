name := "Spark-CluStream"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.2"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.2"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.2"

libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
