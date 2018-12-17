package com.test

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object MyApp {
  private val LOG = org.apache.log4j.Logger.getLogger(MyApp.getClass)
  private val OS = System.getProperty("os.name").toLowerCase

  def main(args: Array[String]): Unit = {
    val runLocal = OS.contains("windows")

    val warehouseLocation = """file:\\\C:\spark-warehouse"""
    val session:SparkSession = if(runLocal) {
      SparkSession
        .builder()
        .appName("MyApp")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .appName("MyApp")
        .config("spark.sql.warehouse.dir", warehouseLocation)
        .enableHiveSupport()
        .getOrCreate()
    }

    val sc = session.sparkContext
    val sqlContext = session.sqlContext

    var mySchema:StructType = null

    try {
      mySchema = sqlContext.sql("select * from test.myTable where 1=0").schema
    } catch {
        case _ => {
          mySchema = StructType(Seq(StructField("id", org.apache.spark.sql.types.DataTypes.IntegerType),
            StructField("name",org.apache.spark.sql.types.DataTypes.StringType)).toArray)
        }
    }

    val df = session.read.csv("""file:\\\""" + System.getProperty("user.dir") + """\\data\\data.csv""")
    df.createOrReplaceTempView("myTempTable")
    sqlContext.sql("create database if not EXISTS test")
    sqlContext.sql("DROP TABLE IF EXISTS test.myTable")
    sqlContext.sql("create table if not exists test.myTable as select * from myTempTable STORED AS PARQUET")
    sqlContext.sql("select * from test.myTable").show(2)

    session.stop()
  }
}