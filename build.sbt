lazy val scala3Version = "3.3.4"
lazy val junitInterfaceVersion = "0.11"
lazy val upickleVersion = "3.1.0"
lazy val logbackversion = "1.4.8"
lazy val nexmarkVersion = "2.41.0"
lazy val pekkoVersion = "1.0.2"
lazy val embeddedKafkaVersion = "3.6.0"

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-explaintypes",
  "-feature",
  "-unchecked",
  "-Wunused:all",
  //  TODO: disabled to avoid compilation errors, to be fixed
  //  "-Xfatal-warnings"
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "execution-service",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    resolvers += "confluent" at "https://packages.confluent.io/maven/", // needed for beam-sdks-java-nexmark
    libraryDependencies += "com.lihaoyi" %% "upickle" % upickleVersion,
    libraryDependencies += "com.novocode" % "junit-interface" % junitInterfaceVersion % Test,
    libraryDependencies += "ch.qos.logback" % "logback-classic" % logbackversion,
    libraryDependencies += "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
    libraryDependencies += "org.apache.beam" % "beam-sdks-java-nexmark" % nexmarkVersion,
    // libraryDependencies += "com.jspenger" %% "sporks3" % "0.1.0-SNAPSHOT",
  )
