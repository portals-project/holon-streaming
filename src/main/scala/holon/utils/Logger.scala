package holon.utils

import org.slf4j.{Logger => SLLogger}

type Logger = SLLogger

object Logger:
  import org.slf4j.{LoggerFactory => SLLoggerFactory}

  import ch.qos.logback.classic.{Level => LBLevel}
  import ch.qos.logback.classic.{LoggerContext => LBLoggerContext}

  setRootLevel("INFO")

  def apply(name: String): Logger =
    SLLoggerFactory.getLogger(name)

  def setLevel(loggerName: String, level: String): Unit =
    val loggerContext: LBLoggerContext = SLLoggerFactory.getILoggerFactory().asInstanceOf[LBLoggerContext]
    val logger: ch.qos.logback.classic.Logger = loggerContext.getLogger(loggerName)
    level match
      case "DEBUG" => logger.setLevel(LBLevel.DEBUG)
      case "INFO"  => logger.setLevel(LBLevel.INFO)
      case "WARN"  => logger.setLevel(LBLevel.WARN)
      case "ERROR" => logger.setLevel(LBLevel.ERROR)
      case "OFF"   => logger.setLevel(LBLevel.OFF)
      case _       => throw new IllegalArgumentException(s"Unknown log level: $level")

  def setRootLevel(level: String): Unit =
    setLevel(SLLogger.ROOT_LOGGER_NAME, level)
