import AssemblyKeys._

libraryDependencies ++= Seq(
	("org.apache.spark" %% "spark-core" % "latest.integration" % "provided"),
  ("org.apache.hadoop" % "hadoop-client" % "1.0.3")
)

assemblySettings

