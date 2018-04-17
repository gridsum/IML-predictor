package com.gridsum.de.impala

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

object FeaturesTableColumns {

  private def getFeatureColumnsFromQueryDetails: List[Column] = {
    val queryDetailColumns = col(TmpColumnNames.QueryDetailColumns)
    List(
      queryDetailColumns.getItem(FeaturesTableColumnNames.SetMem).as(FeaturesTableColumnNames.SetMem),
      queryDetailColumns.getItem(FeaturesTableColumnNames.maxLayer).as(FeaturesTableColumnNames.maxLayer),
      queryDetailColumns.getItem(FeaturesTableColumnNames.maxFiles).as(FeaturesTableColumnNames.maxFiles),
      queryDetailColumns.getItem(FeaturesTableColumnNames.maxSize).as(FeaturesTableColumnNames.maxSize),
      queryDetailColumns.getItem(FeaturesTableColumnNames.events).as(FeaturesTableColumnNames.events),
      queryDetailColumns.getItem(FeaturesTableColumnNames.AggregateCount).as(FeaturesTableColumnNames.AggregateCount),
      queryDetailColumns.getItem(FeaturesTableColumnNames.ExchangeCount).as(FeaturesTableColumnNames.ExchangeCount),
      queryDetailColumns.getItem(FeaturesTableColumnNames.AnalyticCount).as(FeaturesTableColumnNames.AnalyticCount),
      queryDetailColumns.getItem(FeaturesTableColumnNames.SelectCount).as(FeaturesTableColumnNames.SelectCount),
      queryDetailColumns.getItem(FeaturesTableColumnNames.HashJoinCount).as(FeaturesTableColumnNames.HashJoinCount),
      queryDetailColumns.getItem(FeaturesTableColumnNames.NestedLoopJoinCount).as(FeaturesTableColumnNames.NestedLoopJoinCount),
      queryDetailColumns.getItem(FeaturesTableColumnNames.ScanHdfsCount).as(FeaturesTableColumnNames.ScanHdfsCount),
      queryDetailColumns.getItem(FeaturesTableColumnNames.TopNCount).as(FeaturesTableColumnNames.TopNCount),
      queryDetailColumns.getItem(FeaturesTableColumnNames.SortCount).as(FeaturesTableColumnNames.SortCount),
      queryDetailColumns.getItem(FeaturesTableColumnNames.UnionCount).as(FeaturesTableColumnNames.UnionCount)
    )
  }

  private def getFeatureColumnsFromQueryTable: List[Column] =
    List(
      col(QueryDataColumns.QueryID) as FeaturesTableColumnNames.QueryID,
      col(QueryDataColumns.ClusterType) as FeaturesTableColumnNames.ClusterType,
      col(QueryDataColumns.Pool) as FeaturesTableColumnNames.Pool,
      col(QueryDataColumns.User) as FeaturesTableColumnNames.User,
      col(QueryDataColumns.Day) as FeaturesTableColumnNames.Day,
      col(QueryDataColumns.MemoryPerNodePeak) / (1024 * 1024) as FeaturesTableColumnNames.UsedMemInMB
    )

  def getFeatureColumns: List[Column] = {
    getFeatureColumnsFromQueryTable ::: getFeatureColumnsFromQueryDetails
  }


}
