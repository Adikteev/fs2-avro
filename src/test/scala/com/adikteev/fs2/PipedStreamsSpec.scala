package com.adikteev.fs2

import cats.effect.IO
import com.adikteev.fs2.SpecificPipes
import com.aktv.test.UserScore
import fs2.{Stream, io}
import org.apache.avro.file.CodecFactory
import org.specs2.matcher.{IOMatchers, ResultMatchers}
import org.specs2.mutable.Specification

import scala.concurrent.ExecutionContext

class PipedStreamsSpec extends Specification with IOMatchers with ResultMatchers {
  "stream record data using pipedinputstream" >> {
    "should be piped accordingly" >> {
      val scoreFixtures: Seq[UserScore] =
        Seq(new UserScore("aa1", 2.0d, "fr1"), new UserScore("aa2", 2.0d, "fr2"), new UserScore("aa3", 2.0d, "fr3"))

      val avroPipes = SpecificPipes[IO, UserScore]

      val ec = ExecutionContext.global

      val readFileOp: IO[List[UserScore]] = Stream
        .emits(scoreFixtures)
        .through(avroPipes.toAvroFile(ec, CodecFactory.bzip2Codec()))
        .through(avroPipes.fromAvroFile)
        .compile
        .toList

      readFileOp.unsafeRunSync().size should beEqualTo(3)
    }
  }
}
