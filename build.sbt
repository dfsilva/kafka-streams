name := "akka-streams"
organization := "br.com.diegosilva"
version := "0.0.1"
scalaVersion := "3.1.0"


resolvers += Resolver.mavenLocal

libraryDependencies ++= {

  Seq(
     "org.apache.kafka" % "kafka-clients" % "2.8.0",
      "org.apache.kafka" % "kafka-streams" % "2.8.0",
      "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0",
      "io.circe" %% "circe-core" % "0.14.1",
      "io.circe" %% "circe-generic" % "0.14.1",
      "io.circe" %% "circe-parser" % "0.14.1"
  )
}



