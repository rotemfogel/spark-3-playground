package me.rotemfo

import org.scalatest.{BeforeAndAfterEach, FlatSpecLike, Matchers}
import org.slf4j.{Logger, LoggerFactory}

class BaseTest extends FlatSpecLike with Matchers {
  protected val logger: Logger = LoggerFactory.getLogger(getClass)
}

class SimpleTest extends BaseTest {
  "Assert" should "1 equals 1" in {
    assert(1 === 1)
  }
}

class BeforeAndAfterTest extends BaseTest with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    try {
      println("BeforeAndAfterTest")
      throw new Exception("Blat")
    } catch {
      case e: Throwable =>
        logger.error(e.getStackTrace.mkString("\n"))
    }
  }

  "Assert" should "2 equals 2" in {
    assert(2 === 2)
  }
}
