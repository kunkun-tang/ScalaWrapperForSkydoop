name := "ScalaLabs-solutions"

version := "1.0"

scalaVersion := "2.11.2"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
      "junit" % "junit" % "4.7" % "test",
      "org.apache.hadoop" % "hadoop-core" % "1.2.1",
      "com.typesafe" % "config" % "0.4.0"
)



