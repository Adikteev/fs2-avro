package com.adikteev.fs2

import java.lang.reflect.Field

import cats.effect.{ConcurrentEffect, ContextShift}
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord

import scala.reflect.ClassTag

object SpecificPipes {
  def apply[F[_] : ConcurrentEffect : ContextShift, R <: SpecificRecord : ClassTag]: AvroPipes[F, R] =
    new AvroPipes[F, R](SpecificPipes.detectSchema[R])

  private def detectSchema[R <: SpecificRecord : ClassTag]: Schema = {
    val clazz = Class.forName(implicitly[ClassTag[R]].runtimeClass.getCanonicalName)
    val method: Field = clazz.getDeclaredField("SCHEMA$")

    method.get(null).asInstanceOf[Schema]
  }
}
