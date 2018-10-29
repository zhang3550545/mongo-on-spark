package com.maizi

object Test {

  def main(args: Array[String]): Unit = {

    val appName = Props.get("app.name", "Test")

    println(appName)

  }
}