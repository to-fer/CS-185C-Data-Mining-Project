import sbtassembly.Plugin.AssemblyKeys._
import sbt._
import Keys._
import Package.ManifestAttributes

assemblySettings

lazy val mainClassName = "KMeansDriver"

lazy val mainClassString = s"main.$mainClassName"

mainClass := Some(mainClassString)

jarName in assembly := s"$mainClassName.jar"

lazy val baseSettings = Defaults.defaultSettings ++ Seq(
  packageOptions := Seq(ManifestAttributes(
    ("Manifest-Version", "1.0"),
    ("Main-Class", mainClassString))
  )
)


libraryDependencies ++= Seq(
	("org.apache.spark" %% "spark-core" % "0.9.1").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog"),
  "org.apache.hadoop" % "hadoop-client" % "1.0.3" % "provided",
  "com.esotericsoftware.kryo" % "kryo" % "2.24.1-SNAPSHOT"
)

resolvers += "Sonatype" at "https://oss.sonatype.org/content/repositories/snapshots"
