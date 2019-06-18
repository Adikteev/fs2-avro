import aether.AetherKeys._

organization := "com.adikteev.fs2"
version := "0.1-SNAPSHOT"
description := "Stream your avro files with fs2"
normalizedName := "avro"

import Dependencies._

libraryDependencies ++= (fs2 ++ avro ++ specs2)

javaSource in AvroConfig := { sourceManaged.value / "test" }
sourceDirectory in AvroConfig := baseDirectory.value / "src" / "test" / "avro"

credentials in ThisBuild += Credentials(Path.userHome / ".ivy2" / ".packagecloud.credentials")
aetherWagons := Seq(aether.WagonWrapper("packagecloud+https", "io.packagecloud.maven.wagon.PackagecloudWagon"))

publishTo := {
  Some("packagecloud+https" at "packagecloud+https://packagecloud.io/Adikteev/main")
}