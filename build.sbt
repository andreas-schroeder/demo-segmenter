
lazy val commonSettings = Seq(
  version := "1",
  organization := "session-segmenter",
  scalaVersion := "2.12.8",
  resolvers += "Confluent" at "https://packages.confluent.io/maven/",
  libraryDependencies ++= Seq(  "io.confluent" % "kafka-avro-serializer" % "5.1.0",
    "org.apache.kafka" % "kafka-clients" % "2.1.0",
    "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.3",
    "com.github.pureconfig" %% "pureconfig" % "0.10.1",
    "org.apache.logging.log4j" % "log4j-api" % "2.11.1",
    "org.apache.logging.log4j" % "log4j-core" % "2.11.1",
    "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.11.1",
    "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.9.8",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.8",
  )
)

lazy val common = project.settings(commonSettings: _*)

lazy val app = project.dependsOn(common)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= {
      sys.props += "packaging.type" -> "jar"
      Seq(
        "org.apache.kafka" % "kafka-streams" % "2.1.0",
        "org.apache.kafka" %% "kafka-streams-scala" % "2.1.0",
        "org.scalatest" %% "scalatest" % "3.0.5" % "test",
        "org.apache.kafka" % "kafka-streams-test-utils" % "2.1.0" % "test"
      )
    },
    packageName in Docker := "session-segmenter"
  ).enablePlugins(JavaAppPackaging,DockerPlugin)


lazy val driver = project.dependsOn(common)
  .settings(commonSettings: _*)
  .settings(packageName in Docker := "session-segmenter-driver")
  .enablePlugins(JavaAppPackaging,DockerPlugin)

lazy val reader = project.dependsOn(common)
  .settings(commonSettings: _*)
  .settings(packageName in Docker := "session-segmenter-reader")
  .enablePlugins(JavaAppPackaging,DockerPlugin)
