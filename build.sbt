name := "FixrService-Pipeline"

organization := "edu.colorado.plv.fixrservice"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++=  Seq(
  "org.mongodb" %% "casbah" % "3.1.1",
  "com.typesafe.akka" %% "akka-actor" % "2.4.17",
  "com.typesafe.akka" %% "akka-http" % "10.0.6",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.4",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.0.6",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalaj" %% "scalaj-http" % "2.3.0")


//libraryDependencies += "org.mongodb" %% "mongo-scala-driver" % "2.0.0"



        