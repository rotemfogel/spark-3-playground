package me.rotemfo.useragent

import kantan.csv._
import kantan.csv.ops._
import me.rotemfo.common.UserAgentUtils.{parseUserAgent, userAgentFactory}

import java.io.File

object UserAgentApp {
  type Line = (Int, String)

  def main(args: Array[String]): Unit = {

    val rawData: java.net.URL = new File("./data/user_agents.csv").toURI.toURL
    val reader = rawData.asCsvReader[Line](rfc)

    val ua = userAgentFactory
    reader.foreach(line => {
      if (line.isRight) {
        line.foreach({ case (_, uaStr) => println(parseUserAgent(uaStr, ua)) })
      }
    })
  }
}
