name := "spark-clustream"

spName := "obackhoff/spark-clustream"

version := "0.1"

scalaVersion := "2.10.5"

sparkVersion := "1.5.2"

sparkComponents ++= Seq("streaming", "mllib")

libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
