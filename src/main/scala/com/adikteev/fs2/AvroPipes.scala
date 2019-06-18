package com.adikteev.fs2

import java.io._

import cats.effect.{ConcurrentEffect, ContextShift}
import fs2.{Chunk, Pipe, Pull, Sink, Stream}
import org.apache.avro.Schema
import org.apache.avro.file.{CodecFactory, DataFileStream, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, IndexedRecord}
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

/** Collection of avro pipes */
class AvroPipes[F[_] : ConcurrentEffect : ContextShift, R <: IndexedRecord : ClassTag](schema: Schema) {
  import cats.effect._
  import cats.implicits._

  private val F = implicitly[ConcurrentEffect[F]]
  private val classTag = implicitly[ClassTag[R]]

  private val datumReader: DatumReader[R] =
    if (classOf[SpecificRecord].isAssignableFrom(classTag.runtimeClass))
      new SpecificDatumReader[R](schema)
    else
      new GenericDatumReader[R](schema)

  private val datumWriter =
    if (classOf[SpecificRecord].isAssignableFrom(classTag.runtimeClass))
      new SpecificDatumWriter[R](schema)
    else
      new GenericDatumWriter[R](schema)

  /**
    * Takes a byte sink + a schema and forward stream of avro records to that sink.
    * Works for all types of avro records
    */
  def fileThroughSink(
    finalSink: Sink[F, Byte],
    blockingEc: ExecutionContext,
    codecFactory: CodecFactory = CodecFactory.snappyCodec()
  ): Sink[F, R] =
    (stream: Stream[F, R]) => {
      stream
        .through(toAvroFile(blockingEc, codecFactory))
        .through(finalSink)
    }

  /**
    * Builds a optionally compressing pipe of records->byte which will be containing valid avro file bytes
    * It uses a piped (input|output) stream to avoid in-memory bufferization.
    */
  def toAvroFile(
    blockingEc: ExecutionContext,
    codecFactory: CodecFactory = CodecFactory.snappyCodec()
  ): Pipe[F, R, Byte] = { stream: Stream[F, R] =>
    {
      val recordPipe = buildPipe(codecFactory)
        .map {
          case (is, writer) =>
            emitToStream(is, writer)
        }

      Stream
        .eval(recordPipe)
        .flatMap(stream.through)
        .flatMap {
          case (inputStream: InputStream, _) =>
            fs2.io.unsafeReadInputStream(
              F.pure(inputStream.asInstanceOf[InputStream]),
              chunkSize = 1000,
              blockingExecutionContext = blockingEc,
              closeAfterUse = false
            )
        }
    }
  }

  /**
    * Builds a byte to record pipe
    * @todo makes it compatible with compressed avro files
    */
  def fromAvroFile: Pipe[F, Byte, R] = { stream =>
    stream
      .through(fs2.io.toInputStream)
      .through(fromInputStream)
  }

  /**
    * inputstream to records
    * @todo makes it compatible with compressed avro files
    */
  def fromInputStream: Pipe[F, InputStream, R] =
    (stream: Stream[F, InputStream]) => {
      import scala.collection.JavaConverters._

      def nextPull(records: Iterator[R], remaining: Stream[F, InputStream]): Pull[F, R, Unit] =
        if (records.hasNext) {
          Pull.output(Chunk.seq(records.take(500).toSeq)) >> nextPull(records, remaining)
        } else {
          doneOrLoop()(remaining)
        }

      def doneOrLoop()(s: Stream[F, InputStream]): Pull[F, R, Unit] =
        s.pull.uncons1.flatMap {
          case Some((s, str)) =>
            val records = new DataFileStream[R](s, datumReader)
            nextPull(records.iterator().asScala, str)
          case None => Pull.done
        }

      doneOrLoop()(stream).stream
    }

  /**
    * Internal: build a pipe that return a pipedoutputstream and also run a concurently appending operation.
    */
  private def emitToStream(
    is: PipedInputStream,
    writer: DataFileWriter[R]
  ): Pipe[F, R, (PipedInputStream, DataFileWriter[R])] = { recordStream =>
    Stream
      .emit[F, (PipedInputStream, DataFileWriter[R])]((is, writer))
      .concurrently(
        recordStream
          .evalTap(
            record =>
              F.delay {
                writer.append(record)
              }
          )
          .onFinalize(F.delay(writer.close()))
      )
  }

  /** Build a data file writer that is preconfigured with a codec */
  private def buildWriter(codecFactory: CodecFactory)(implicit F: Sync[F]): F[DataFileWriter[R]] =
    F.delay(
      new DataFileWriter[R](datumWriter)
        .setCodec(codecFactory)
    )

  private def buildPipe(codecFactory: CodecFactory)(implicit F: Sync[F]): F[(PipedInputStream, DataFileWriter[R])] =
    buildWriter(codecFactory)
      .flatMap(
        writer =>
          F.delay {
            val input = new PipedInputStream()
            val output = new PipedOutputStream(input)

            (input, writer.create(schema, output))
          }
      )
}
