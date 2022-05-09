package me.rotemfo.common

import scopt.OptionParser

case class ReaderConfig(inputLocation: Option[String] = None, outputLocation: Option[String] = None)

// @formatter:off
object ReaderParser extends OptionParser[ReaderConfig](programName = "ParquetFileReaderApp Application") {
  opt[String]("input-location") .required.action { (x, p) => p.copy(inputLocation  = Some(x)) }
    .validate(x => if (x.nonEmpty) success else failure("input-location cannot be empty"))
  opt[String]("output-location").required.action { (x, p) => p.copy(outputLocation = Some(x)) }
}
// @formatter:on
