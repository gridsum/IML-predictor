package com.gridsum.de.impala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object Features {
  val partNum = 20

  private def getFeaturesDFFromQueryDataDF(queryDataDF: DataFrame, impalaVersion: String) = {
    queryDataDF
      .withColumn(TmpColumnNames.QueryDetailColumns, FeatureExtractionUDF.getFeatureColumnsFromQueryDetail(impalaVersion)(col(QueryDataColumns.QueryDetailInfo)))
      .select(FeaturesTableColumns.getFeatureColumns: _*)
  }

  def main(args: Array[String]): Unit = {

    val startDay = args(0)
    val endDay = args(1)
    val impalaVersion = args(2)
    val outputPath = args(3)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    try {
      val queryDataDF = new QueryDataTable(startDay, endDay, sc).getQueryDataDF

      val featuresDF = getFeaturesDFFromQueryDataDF(queryDataDF, impalaVersion)

      FileSystem.get(new Configuration()).delete(new Path(outputPath), true)
      featuresDF.repartition(partNum).write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .save(outputPath)
    }
    finally {
      sc.stop()
    }
    println("Finished Features Task")
  }
}

