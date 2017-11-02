resolvers ++= Seq(
  Resolver sonatypeRepo "public",
  Resolver typesafeRepo "releases",
)

name := "spark-clustream"

//spName := "obackhoff/spark-clustream"

version := "0.1"

scalaVersion := "2.11.8"

//sparkVersion := "2.2.0"

//sparkComponents ++= Seq("streaming", "mllib")

libraryDependencies ++= Seq(
"com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),
"org.apache.spark" % "spark-mllib_2.11" % "2.2.0",
"org.apache.spark" % "spark-streaming_2.11" % "2.2.0"
)
