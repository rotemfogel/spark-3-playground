package me.rotemfo.common

import scopt.OptionParser

abstract class ParquetReaderApplication
  extends LocalBaseApplication[ReaderConfig](ReaderConfig()) {

  override protected def getParser: OptionParser[ReaderConfig] = ReaderParser

}
