package com.maizi

import org.apache.log4j.{Level, Logger}

trait Logging extends Serializable {
  private val clazz = this.getClass
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark_project.jetty").setLevel(Level.OFF)

  lazy val logger = Logger.getLogger(clazz)
}