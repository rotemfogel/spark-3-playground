package me.rotemfo.common

import scopt.OptionParser

case class EmptyConfig()

object EmptyConfigParser extends OptionParser[EmptyConfig](programName = "Empty Application")