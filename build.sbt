import AssemblyKeys._

libraryDependencies ++= Seq(
	("org.apache.spark" %% "spark-core" % "latest.integration" % "provided").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog"),
  ("org.apache.hadoop" % "hadoop-client" % "1.0.3" % "provided")
)

assemblySettings

