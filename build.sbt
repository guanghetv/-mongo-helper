name := "mongo-helper"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "org.mongodb.scala" %% "mongo-scala-driver" % "1.1.1",
  "org.json4s" %% "json4s-native" % "3.4.0"
)