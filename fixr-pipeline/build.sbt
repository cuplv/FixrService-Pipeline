name := "fixr-pipeline"

version := "1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "edu.colorado.plv.fixr" %% "bigglue" % "1.0-SNAPSHOT",
  "com.typesafe.akka" %% "akka-actor" % "2.4.19"
)
        