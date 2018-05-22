enablePlugins(SiteScaladocPlugin)
enablePlugins(GhpagesPlugin)

inThisBuild(List(
  autoAPIMappings := true
))

organization := "edu.colorado.plv"

name := "bigglue"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.4"

scalacOptions in (Compile, doc) ++= Seq(
  "-groups", // Group similar functions together (based on the @group annotation)
  "-implicits" // Document members inherited by implicit conversions.
)

siteSubdirName in SiteScaladoc := "api/latest"

scmInfo := Some(ScmInfo(url("https://github.com/cuplv/FixrService-Pipeline"), "https://github.com/cuplv/FixrService-Pipeline.git"))
git.remoteRepo := scmInfo.value.get.connection

libraryDependencies ++=  Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.17",
  "com.typesafe.akka" %% "akka-remote" % "2.4.17",
  "com.typesafe.akka" %% "akka-http" % "10.0.6",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.4",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.0.6",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalaj" %% "scalaj-http" % "2.3.0")


libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.11.0.0"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.11.0.0"

