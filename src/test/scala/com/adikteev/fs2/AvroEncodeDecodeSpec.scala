package com.adikteev.fs2

import java.io.File

import cats.effect.IO
import com.adikteev.fs2.SpecificPipes
import com.aktv.test.UserScore
import fs2.Stream
import org.apache.avro.file.DataFileReader
import org.apache.avro.generic.GenericDatumReader
import org.specs2.matcher.{IOMatchers, ResultMatchers}
import org.specs2.mutable.Specification

import scala.concurrent.ExecutionContext

class AvroEncodeDecodeSpec extends Specification with IOMatchers with ResultMatchers {
  "should be able to store avro records to a (closed) file" >> {
    "test file was stored" >> {
      import scala.collection.JavaConverters._

      val scoreFixtures: Seq[UserScore] =
        Seq(new UserScore("aa", 2.0d, "fr"), new UserScore("bb", 2.0d, "uk"), new UserScore("cc", 2.0d, "toto"))

      val temp = File.createTempFile("tempfile", ".avro")

      val ec = ExecutionContext.global

      val avroPipes = SpecificPipes.apply[IO, UserScore]

      Stream
        .emits(scoreFixtures)
        .to(avroPipes.fileThroughSink(fs2.io.file.writeAll(temp.toPath, ec), ec))
        .compile
        .drain
        .unsafeRunSync()

      val reader: DataFileReader[Nothing] = new DataFileReader[Nothing](temp, new GenericDatumReader[Nothing])

      val metadatas = reader.getMetaKeys.asScala
        .map(key => (key, reader.getMetaString(key)))

      metadatas must havePair(
        "avro.schema" -> """{"type":"record","name":"UserScore","namespace":"com.aktv.test","fields":[{"name":"advertising_id","type":"string"},{"name":"conversion_rate","type":"double"},{"name":"country","type":"string"}]}"""
      )
      metadatas must havePair("avro.codec" -> "snappy")

      temp.exists() must beTrue

    }
  }
}
