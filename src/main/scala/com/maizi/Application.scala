package com.maizi

import java.util.Properties

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  *
  * Application类介绍：读取mongo打平后的数据，通过sparksql，定义Schema，写入mysql
  *
  * 需要传入的参数：
  *
  * 1. 需要通过 jvm 参数传入 -DPropPath="/xxx/application.properties" 配置文件
  *
  * 2. 需要传入3个参数
  * 参数1：table name,即是mongodb的collection，也是mysql的table
  * 参数2：columns，即mongodb中的需要提取的field名称，用来做schema约束
  * 参数3：sql，执行的sparksql读取mongo的sql语句
  *
  *
  */
object Application extends Logging {

  val TEMP_TABLE: String = "temp"
  val APP_NAME: String = "app.name"
  val ENV: String = "run.env"
  val MASTER: String = "master"

  val MONGODB_URI: String = "spark.mongodb.input.uri"
  val MONGODB_DB: String = "spark.mongodb.input.database"
  val MONGODB_COLLECTION: String = "spark.mongodb.input.collection"
  val MONGO_FORMAT: String = "com.mongodb.spark.sql"

  val MYSQL_URI: String = "mysql.uri"
  val MYSQL_USER: String = "mysql.user"
  val MYSQL_PWD: String = "mysql.password"


  /**
    * 获取schema
    *
    * @param columns 需要的字段名称
    * @return 返回一个schema对象
    */
  def getSchema(columns: String): StructType = {
    val list: Array[String] = columns.split(",")
    val listField: ListBuffer[StructField] = new ListBuffer()

    for (x <- list.indices) {
      val column = list(x)
      val field: StructField = StructField(column, StringType)
      listField.append(field)
    }

    StructType(listField)
  }

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      logger.info("参数不对，程序退出")
      System.exit(0)
    }

    val tableName = args(0)
    val columns = args(1)
    val sql = args(2)

    logger.info("tableName: " + tableName)
    logger.info("columns: " + columns)
    logger.info("sql: " + sql)

    val appName = Props.get(APP_NAME, "Application")
    val env = Props.get(ENV, "dev")
    val master = Props.get(MASTER, "local")

    logger.info("appName: " + appName)
    logger.info("env: " + env)
    logger.info("master: " + master)

    val mongoUri = Props.get(MONGODB_URI, "")
    val mongoDB = Props.get(MONGODB_DB, "")

    logger.info("mongoUri: " + mongoUri)
    logger.info("mongoDB: " + mongoDB)

    val mysqlUri = Props.get(MYSQL_URI, "")
    val mysqlUser = Props.get(MYSQL_USER, "")
    val mysqlPwd = Props.get(MYSQL_PWD, "")

    logger.info("mysqlUri: " + mysqlUri)
    logger.info("mysqlUser: " + mysqlUser)
    logger.info("mysqlPwd: " + mysqlPwd)


    val spark = SparkSession.builder()
      .appName(appName)
      .master(master)
      .config(MONGODB_URI, mongoUri)
      .config(MONGODB_DB, mongoDB)
      .config(MONGODB_COLLECTION, tableName)
      .getOrCreate()


    if (env == "dev") {
      // 设置log级别
      spark.sparkContext.setLogLevel("WARN")
    }


    val schema = getSchema(columns)
    logger.info("schema: " + schema)

    // 通过schema约束，直接获取需要的字段
    val df = spark.read.format(MONGO_FORMAT).schema(schema)
      .option("spark.mongodb.input.partitioner", "MongoPaginateByCountPartitioner")
      .option("spark.mongodb.input.partitionerOptions.partitionKey", "BD@Mz@channelName")
      .option("spark.mongodb.input.partitionerOptions.numberOfPartitions", "10")
      .load()

    df.createOrReplaceTempView(tableName)

    val resDf = spark.sql(sql)

    resDf.show(10)

    val prop = new Properties()
    prop.setProperty("user", mysqlUser)
    prop.setProperty("password", mysqlPwd)
    resDf.write.mode(SaveMode.Append).jdbc(mysqlUri, tableName, prop)

    spark.stop()
    System.exit(0)
  }
}