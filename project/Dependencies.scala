import sbt._

object Dependencies {
  val fs2V = "1.0.2"
  val specs2V = "4.5.1"
  val avroV = "1.8.2"
  val sloggingV = "3.9.2"

  val fs2 = Seq("co.fs2"           %% "fs2-core" % fs2V, "co.fs2"           %% "fs2-io"    % fs2V)
  val avro = Seq("org.apache.avro" % "avro"      % avroV, "org.apache.avro" % "avro-tools" % avroV % Test)

  val specs2 = Seq(
    "org.specs2" %% "specs2-matcher"  % specs2V % Test,
    "org.specs2" %% "specs2-fp"       % specs2V % Test,
    "org.specs2" %% "specs2-analysis" % specs2V % Test,
    "org.specs2" %% "specs2-cats"     % specs2V % Test
  )
  val scalaLogging = Seq("com.typesafe.scala-logging" %% "scala-logging" % sloggingV)
}
