name := "pipelineCombinators"

version := "1.0"

scalaVersion := "2.12.2"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

// (optional) If you need scalapb/scalapb.proto or anything from
// google/protobuf/*.proto

libraryDependencies ++=  Seq(
  "com.typesafe" % "config" % "1.3.0",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalaj" %% "scalaj-http" % "2.3.0",
  "edu.colorado.plv.fixr" %% "mthread_abstrac" % "0.10-SNAPSHOT",
  "com.trueaccord.scalapb" %% "scalapb-runtime" % com.trueaccord.scalapb.compiler.Version.scalapbVersion % "protobuf"
)
