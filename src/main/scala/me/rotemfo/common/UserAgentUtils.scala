package me.rotemfo.common

import me.rotemfo.common.functions.{mergeMaps, toJson}
import nl.basjes.parse.useragent.UserAgent._
import nl.basjes.parse.useragent.{UserAgent, UserAgentAnalyzer}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.matching.Regex

object UserAgentUtils {
  private final lazy val wrapperRegex: Regex = "^sa-.*-wrapper.*".r
  private final lazy val isWebWrapper: String => Boolean = x => wrapperRegex.findFirstIn(x).isDefined
  private final lazy val oldWrapperRegex: Regex = "com.seekingalpha.webwrapper.*".r
  private final lazy val isOldWrapper: String => Boolean = x => oldWrapperRegex.findFirstIn(x).isDefined
  private final lazy val oldAppWrapper: Regex = ".*SeekingAlpha.*App.*".r
  private final lazy val isOldApp: String => Boolean = x => oldAppWrapper.findFirstIn(x).isDefined
  private final lazy val androidWrapper: String = "sa-android-samw-wrapper"

  private sealed trait SaParser {
    def invoke(parts: Array[String]): Map[String, String]
  }

  private trait CommonParser extends SaParser {
    def getCommon(parts: Array[String]): Map[String, String] = {
      val agentData = parts(0).split("/")
      val agentName = agentData.head.toLowerCase
      val agentVersion = agentData.last.split("\\(").head
      val agentNameVersion = s"$agentName $agentVersion"
      Map(AGENT_NAME -> agentName,
        AGENT_VERSION -> agentVersion,
        AGENT_NAME_VERSION -> agentNameVersion,
        AGENT_NAME_VERSION_MAJOR -> agentNameVersion)
    }
  }

  private case object MobileWebParser extends CommonParser {
    override def invoke(parts: Array[String]): Map[String, String] = {
      val osName = parts(3)
      val osVersion = parts(5).split(";").head
      getCommon(parts) ++ Map(OPERATING_SYSTEM_NAME -> osName, OPERATING_SYSTEM_VERSION -> osVersion)
    }
  }

  private case object MobileAppParser extends CommonParser {
    override def invoke(parts: Array[String]): Map[String, String] = {
      val (osName, osVersion) =
        if (parts(0).startsWith(androidWrapper)) {
          val osName = parts(1).split("\\(").last.split(";").head
          val osVersion = parts.last.split("\\)").head
          (osName, osVersion)
        }
        else { // iosWrapper
          val osData = parts.last.split("/")
          val osName = osData.head
          val osVersion = osData.last.split("\\)").head
          (osName, osVersion)
        }
      getCommon(parts) ++ Map(OPERATING_SYSTEM_NAME -> osName, OPERATING_SYSTEM_VERSION -> osVersion)
    }
  }

  private case object OldMobileAppParser extends SaParser {
    override def invoke(parts: Array[String]): Map[String, String] = {
      val osData = parts(6).split("/")
      val osName = osData.head match {
        case x if x.toLowerCase() == "ios" => "iOS"
        case x if x.toLowerCase() == "android" => "Android"
        case x => x
      }
      val osVersion = osData.last
      val agentVersion = parts(9).split("/").last
      val agentName = parts(10).split("/").last
      val agentNameVersion = s"$agentName $agentVersion"
      val osNameVersion = s"$osName $osVersion"
      Map(
        AGENT_NAME -> agentName,
        AGENT_VERSION -> agentVersion,
        AGENT_NAME_VERSION -> agentNameVersion,
        AGENT_NAME_VERSION_MAJOR -> agentNameVersion,
        OPERATING_SYSTEM_NAME -> osName,
        OPERATING_SYSTEM_VERSION -> osVersion,
        OPERATING_SYSTEM_NAME_VERSION -> osNameVersion,
        OPERATING_SYSTEM_NAME_VERSION_MAJOR -> osNameVersion,
        OPERATING_SYSTEM_VERSION_MAJOR -> osVersion
      )
    }
  }

  /**
   * get the parser to invoke
   *
   * @param userAgentString - the UserAgent string
   * @return - Option ParserType
   */
  private def getParserType(userAgentString: String): Option[SaParser] = {
    if (isWebWrapper(userAgentString))
      Some(MobileAppParser)
    else if (isOldWrapper(userAgentString))
      Some(MobileWebParser)
    else if (isOldApp(userAgentString))
      Some(OldMobileAppParser)
    else None
  }

  private def saParseUserAgent(userAgentString: String): Map[String, String] = {
    val parserType: Option[SaParser] = getParserType(userAgentString)
    if (parserType.isEmpty) Map()
    else {
      val parts = userAgentString.split(" ")
      // invoke the parser and get result
      parserType.get.invoke(parts)
    }
  }

  /**
   *
   * @param userAgentString - The UserAgent string
   * @param ua              - The UserAgent Analyzer
   * @return - Option[String]
   */
  def parseUserAgent(userAgentString: String, ua: UserAgentAnalyzer): Option[String] = {
    if (userAgentString.isEmpty) None
    else {
      val agent: UserAgent = ua.parse(userAgentString)
      val uaDetails: Map[String, String] = {
        val map: Map[String, String] = agent.getAvailableFieldNamesSorted.asScala
          .map(field => (field, agent.getValue(field)))
          .toMap[String, String]
        val maybeParsed = Try(saParseUserAgent(userAgentString))
        if (maybeParsed.isSuccess) {mergeMaps(map, maybeParsed.get)} else map
      }
      Some(toJson(uaDetails.mapValues(v => if (v.equals("??")) "Unknown" else v)))
    }
  }

  val userAgentFactory: UserAgentAnalyzer = userAgentFactory(-1, true)

  def userAgentFactory(cacheSize: Int = 0,
                       preHeat: Boolean = false): UserAgentAnalyzer = {
    var builder = UserAgentAnalyzer.newBuilder()
    if (cacheSize > 0)
      builder = builder.withCache(cacheSize)
    else
      builder = builder.withoutCache()
    if (preHeat && cacheSize > 0) {
      builder = builder.preheat(cacheSize)
    }
    builder.build()
  }
}
