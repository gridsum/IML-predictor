package com.gridsum.de.impala

import org.apache.spark.sql.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.mutable.ArrayBuffer
import scala.math.{pow, round}


object FeatureExtractionUDF {
  val queryDetailSep = "------------------------------------------------------------------------------------------------------"
  val explainSep = "Query Options"
  val filesPattern = "files=(\\d+)".r
  val sizePattern = "size=([\\d.]*)(.*)".r
  val hostsPattern = "hosts=(\\d+)".r
  val setMemPattern = " MEM_LIMIT=([\\d.]*)".r

  val units = Map("PB" -> pow(1024, 3), "TB" -> pow(1024, 2),
    "GB" -> pow(1024, 1), "MB" -> pow(1024, 0),
    "KB" -> pow(1024, -1), "B" -> pow(1024, -2))

  def getSetMem(queryDetail: String): Int = {
    val setMemMB = setMemPattern.findFirstIn(queryDetail).map {
      case (setMemPattern(num)) => (num.toLong * units("B")).toInt
    }.getOrElse(0)
    setMemMB
  }

  def getFeatures(queryDetail: String, impalaVersion: String): Map[String, Int] = {
    //获取explain内容
    val explainStr = queryDetail.split(explainSep).drop(1).mkString
    val explainLines = explainStr.split("\n")

    //过滤出explain中每一小段落的开头行，例如06:TOP-N [LIMIT=10]
    val explainSummary = explainLines.filter(line => line.contains(":") && (line.split(":").length > 1) &&
      line.split(":")(1).head <= 'Z' && line.split(":")(1).head >= 'A')

    //过滤出explain中包含浏览的文件数目和文件大小的信息
    val filesInfo = explainLines.filter(line => line.contains("partitions") ||
      line.contains("hosts"))

    //获取explain中包含了多少个小段落
    val events = explainSummary.length

    val (mLayer, mSize, mFiles) = try{
      if (impalaVersion == "2.9.0-cdh5.12.1") getFilesNew(filesInfo) else getFilesOld(filesInfo)
    }catch {
      case e => throw new RuntimeException(e.toString+" \n  detail is "+queryDetail)
    }
    //获取浏览文件的信息

    val explainSummaryStr = explainSummary.mkString("")
    getKeywords(explainSummaryStr) ++ Map(FeaturesTableColumnNames.events -> events,
      FeaturesTableColumnNames.maxLayer -> mLayer,
      FeaturesTableColumnNames.maxSize -> mSize,
      FeaturesTableColumnNames.maxFiles -> mFiles)
  }

  /**
    * 获取explain信息中的关键词出现的个数
    */
  def getKeywords(explainSummaryStr: String): Map[String, Int] = {
    val AGGREGATE = ":AGGREGATE"
    val EXCHANGE = ":EXCHANGE"
    val ANALYTIC = ":ANALYTIC"
    val SELECT = ":SELECT"
    val HASH_JOIN = ":HASH JOIN"
    val NESTED_LOOP_JOIN = ":NESTED LOOP JOIN"
    val SCAN_HDFS = ":SCAN HDFS"
    val TOP_N = ":TOP-N"
    val SORT = ":SORT"
    val UNION = ":UNION"

    Map(
      FeaturesTableColumnNames.AggregateCount -> countSubString(explainSummaryStr, AGGREGATE),
      FeaturesTableColumnNames.ExchangeCount -> countSubString(explainSummaryStr, EXCHANGE),
      FeaturesTableColumnNames.AnalyticCount -> countSubString(explainSummaryStr, ANALYTIC),
      FeaturesTableColumnNames.SelectCount -> countSubString(explainSummaryStr, SELECT),
      FeaturesTableColumnNames.HashJoinCount -> countSubString(explainSummaryStr, HASH_JOIN),
      FeaturesTableColumnNames.NestedLoopJoinCount -> countSubString(explainSummaryStr, NESTED_LOOP_JOIN),
      FeaturesTableColumnNames.ScanHdfsCount -> countSubString(explainSummaryStr, SCAN_HDFS),
      FeaturesTableColumnNames.TopNCount -> countSubString(explainSummaryStr, TOP_N),
      FeaturesTableColumnNames.SortCount -> countSubString(explainSummaryStr, SORT),
      FeaturesTableColumnNames.UnionCount -> countSubString(explainSummaryStr, UNION)
    )
  }

  def countSubString(source: String, sub: String): Int = {
    source.split(sub).length - 1
  }

  /**
    * 获取explain树的嵌套深度，以及除了最右子树外的所有子树中的最大的浏览文件数和文件大小
    */
  def getFilesNew(filesInfo: Array[String]): (Int, Int, Int) = {
    val lineLayer = filesInfo.map(line => line.count(_ == '|') + 1)
    val maxLayer = lineLayer.reduceLeft(_ max _)
    //如果树只有一层，那么直接返回层数1，文件数和文件大小都返回0
    if (maxLayer == 1) {
      return (1, 0, 0)
    }
    var layerInstancesCounts = ArrayBuffer[(Int, Int)]()
    var maxSize = 0
    var maxFiles = 0
    lineLayer.zip(filesInfo).filter(t => t._1 != maxLayer).foreach { case (layer, fileLine) =>
      if (fileLine.contains("instances=")) {
        val instances = fileLine.split("instances=")(1).toInt
        layerInstancesCounts.append((layer, instances))
      } else {
        var files = filesPattern.findFirstIn(fileLine).map {
          case filesPattern(i) => i.toDouble
        }.getOrElse(0.0)
        var size = sizePattern.findFirstIn(fileLine).map {
          case sizePattern(num, unit) => num.toDouble * units(unit)
        }.getOrElse(0.0)
        val instances = getLayerInstant(layerInstancesCounts, layer)
        size = size / instances
        files = files / instances
        if (size > maxSize) {
          maxSize = round(size).toInt
          maxFiles = round(files).toInt
        }
      }
    }
    (maxLayer, maxSize, maxFiles)
  }

  def getFilesOld(filesInfo: Array[String]): (Int, Int, Int) = {
    val lineLayer = filesInfo.map(line => line.count(_ == '|') + 1)
    val maxLayer = lineLayer.reduceLeft(_ max _)
    //如果树只有一层，那么直接返回层数1，文件数和文件大小都返回0
    if (maxLayer == 1) {
      return (1, 0, 0)
    }
    var maxSize = 0
    var maxFiles = 0
    val layerFilesInfo = lineLayer.zip(filesInfo).filter(t => t._1 != maxLayer)
    for (((layer, fileLine), index) <- layerFilesInfo.view.zipWithIndex ){
      if (fileLine.contains("partitions")) {
        var files = filesPattern.findFirstIn(fileLine).map {
          case filesPattern(i) => i.toDouble
        }.getOrElse(0.0)
        var size = sizePattern.findFirstIn(fileLine).map {
          case sizePattern(num, unit) => num.toDouble * units(unit)
        }.getOrElse(0.0)
        var hostsLine = layerFilesInfo.drop(index+1).filter(t => t._2.contains("hosts")).head._2
        var hosts = hostsPattern.findFirstIn(hostsLine).map {
          case hostsPattern(i) => i.toInt
        }.getOrElse(1)
        size = size / hosts
        files = files / hosts
        if (size > maxSize) {
          maxSize = round(size).toInt
          maxFiles = round(files).toInt
        }
      }
    }
    (maxLayer, maxSize, maxFiles)
  }

  def getLayerInstant(array: ArrayBuffer[(Int, Int)], value: Int): Int = {
    array.filter(t => t._1 <= value).last._2
  }

  def getFeatureColumnsFromQueryDetail(impalaVersion:String): UserDefinedFunction = udf((queryDetailInfo: String) => {
    val queryDetail = queryDetailInfo.split(queryDetailSep)(0)
    Map(FeaturesTableColumnNames.SetMem -> getSetMem(queryDetail)) ++ getFeatures(queryDetail, impalaVersion)
  })
}