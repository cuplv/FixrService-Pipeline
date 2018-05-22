name := "fixr-pipeline"

version := "1.0"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "edu.colorado.plv" %% "bigglue" % "1.0-SNAPSHOT",
  "com.typesafe.akka" %% "akka-actor" % "2.4.19",
  "org.scala-lang.modules" %% "scala-xml" % "1.0.6"
)
        