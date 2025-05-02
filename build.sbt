lazy val scala3Version = "3.3.4"
lazy val junitInterfaceVersion = "0.11"
lazy val upickleVersion = "3.1.0"
lazy val logbackversion = "1.4.8"
lazy val nexmarkVersion = "2.41.0"
lazy val pekkoVersion = "1.0.2"
lazy val embeddedKafkaVersion = "3.6.0"
val firestoreVersion = "3.31.1"
val grpcVersion = "1.72.0"
val gaxVersion = "2.64.0"

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

ThisBuild / javaOptions ++= Seq(
    "-XX:+PrintGCDetails",
    "-XX:+PrintGCDateStamps",
    "-Xloggc:gc.log",
    "-XX:+UseGCLogFileRotation",
    "-XX:NumberOfGCLogFiles=5",
    "-XX:GCLogFileSize=10M"
    )

lazy val root = project
  .in(file("."))
  .settings(
    name := "execution-service",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    resolvers += "confluent" at "https://packages.confluent.io/maven/", // needed for beam-sdks-java-nexmark
    libraryDependencies += "com.lihaoyi" %% "upickle" % upickleVersion,
    libraryDependencies += "ch.qos.logback" % "logback-classic" % logbackversion,
    libraryDependencies += "io.github.embeddedkafka" %% "embedded-kafka" % embeddedKafkaVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-actor-typed" % pekkoVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-cluster-typed" % pekkoVersion,
    libraryDependencies += "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
    libraryDependencies += "org.apache.beam" % "beam-sdks-java-nexmark" % nexmarkVersion,
    libraryDependencies += "com.google.cloud" % "google-cloud-storage" % "2.28.0",
    libraryDependencies += "com.google.cloud" % "google-cloud-firestore" % "3.0.6",
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.11",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test,
// libraryDependencies += "com.jspenger" %% "sporks3" % "0.1.0-SNAPSHOT",
  )

libraryDependencies ++= Seq(
    // Google Cloud
    "com.google.cloud" % "google-cloud-storage" % "2.28.0",
    "com.google.cloud" % "google-cloud-firestore" % firestoreVersion,
    "com.google.api" % "gax-grpc" % gaxVersion,

    // gRPC dependencies
    "io.grpc" % "grpc-core" % grpcVersion,
    "io.grpc" % "grpc-netty" % grpcVersion,
    "io.grpc" % "grpc-netty-shaded" % grpcVersion,
    "io.grpc" % "grpc-okhttp" % grpcVersion,
    "io.grpc" % "grpc-protobuf" % grpcVersion,
    "io.grpc" % "grpc-api" % grpcVersion,
    "io.grpc" % "grpc-stub" % grpcVersion,
    "io.grpc" % "grpc-auth" % grpcVersion,
    "io.grpc" % "grpc-context" % grpcVersion,

    // Logging
    "org.slf4j" % "slf4j-api" % "2.0.9",
    "ch.qos.logback" % "logback-classic" % "1.2.11"
    )

enablePlugins(AssemblyPlugin)

//assemblyMergeStrategy in assembly := {
//    case PathList("META-INF", "services", _*) => MergeStrategy.concat
//    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//    case x => MergeStrategy.first
//}

assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "services", _*) => MergeStrategy.concat
    case PathList("reference.conf") => MergeStrategy.concat
    case PathList("application.conf") => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
}