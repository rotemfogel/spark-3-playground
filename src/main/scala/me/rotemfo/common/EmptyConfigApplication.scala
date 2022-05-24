package me.rotemfo.common

import scopt.OptionParser

abstract class EmptyConfigApplication extends LocalBaseApplication[EmptyConfig](EmptyConfig()) {
  override protected def getParser: OptionParser[EmptyConfig] = EmptyConfigParser
}
