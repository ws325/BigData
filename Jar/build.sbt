name := "Movies"

version := "0.1"

scalaVersion := "2.12.15"

idePackagePrefix := Some("agh")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.3",
  "org.apache.spark" %% "spark-sql" % "3.1.3"
)