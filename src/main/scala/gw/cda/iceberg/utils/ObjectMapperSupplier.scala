package gw.cda.iceberg.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/** Cached Jackson mappers (JSON + YAML) with the Scala module registered.
  * One instance per format is reused throughout the codebase.
  *
  * Uses `ObjectMapper` directly (no `ScalaObjectMapper` mixin — that
  * trait was deprecated in Jackson 2.13 and removed in newer versions).
  * Type-erased reads still work; pass an explicit `classOf[T]` /
  * `TypeReference` at call sites that need it.
  */
object ObjectMapperSupplier {
  val jsonMapper: ObjectMapper = {
    val m = new ObjectMapper()
    m.registerModule(DefaultScalaModule)
    m
  }

  val yamlMapper: ObjectMapper = {
    val m = new ObjectMapper(new YAMLFactory)
    m.registerModule(DefaultScalaModule)
    m
  }
}
