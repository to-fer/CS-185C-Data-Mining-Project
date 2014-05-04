import AssemblyKeys._
import sbt._
import Keys._
import Package.ManifestAttributes

assemblySettings

lazy val mainClassString = "main.Main"

mainClass := Some(mainClassString)

lazy val baseSettings = Defaults.defaultSettings ++ Seq(
  packageOptions := Seq(ManifestAttributes(
    ("Manifest-Version", "1.0"),
    ("Main-Class", mainClassString))
  )
)


libraryDependencies ++= Seq(
	("org.apache.spark" %% "spark-core" % "latest.integration" % "provided").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog"),
  ("org.apache.hadoop" % "hadoop-client" % "1.0.3" % "provided")
)
