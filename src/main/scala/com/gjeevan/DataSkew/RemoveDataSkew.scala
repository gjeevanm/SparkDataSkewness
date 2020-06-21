package com.gjeevan.RemoveDataSkew

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, concat, explode, floor, lit, rand}

object RemoveDataSkew extends App {

  val sparkconf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("RemoveDataSkewness")

  val spark = SparkSession
    .builder()
    .config(sparkconf)
    .getOrCreate()

  import spark.implicits._

  val df1 = Seq(
    ("x", "bc"),
    ("x", "ce"),
    ("x", "ab"),
    ("x", "ef"),
    ("x", "gh"),
    ("y", "hk"),
    ("z", "jk")
  ).toDF()
  df1.show(10,false)

  val df2 = Seq(
    ("x", "gkl"),
    ("y", "nmb"),
    ("z", "qwe")
  ).toDF()

  df2.show(10,false)

  def elimnateDataSkew(leftTable: DataFrame, leftCol: String, rightTable: DataFrame) = {

    var df1 = leftTable
      .withColumn(leftCol, concat(
        leftTable.col(leftCol), lit("_"), lit(floor(rand(123456) * 10))))
    var df2 = rightTable
      .withColumn("explodedCol",
        explode(
          array((0 to 10).map(lit(_)): _ *)
        ))

    (df1, df2)
  }

  val (df3, df4) = elimnateDataSkew(df1, "_1", df2)

  df3.show(100, false)
  df4.show(100, false)


}
