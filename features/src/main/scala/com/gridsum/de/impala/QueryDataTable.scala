package com.gridsum.de.impala

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive.HiveContext

class QueryDataTable(val startDay: String,
                     val endDay: String,
                     val sc: SparkContext) {
  val ComputeStatsSessionID = "compute_stats_session_id"

  def getQueryDataDF: DataFrame = {
    val hiveContext = new HiveContext(sc)
    val df = hiveContext.sql(QueryDataTable.QueryDataSql.format(startDay, endDay))
    val compute_stats_df = hiveContext.sql(QueryDataTable.ComputeStatsSql.format(startDay, endDay))

    val queryDataWithoutComputeStatsDF = df.join(compute_stats_df.select(col(QueryDataColumns.SessionID).alias(ComputeStatsSessionID)),
      col(QueryDataColumns.SessionID) === col(ComputeStatsSessionID),
      "leftouter").where(col(ComputeStatsSessionID).isNull).drop(col(ComputeStatsSessionID))

    queryDataWithoutComputeStatsDF
  }
}

object QueryDataTable {
  val QueryDataSql =
    s"""
              select session_id, query_id,cluster_type,pool,user,day,memory_per_node_peak,
              `database`,statement,query_detail_info from dataengineering.impala_query_info where day>=%s
              and day<=%s and user!="hadoop-monitor" and query_type="QUERY" and query_state="FINISHED" and
              session_type="HIVESERVER2" and memory_per_node_peak>0 and length(query_detail_info)>0
      """

  val ComputeStatsSql =
    s"""
              select session_id from dataengineering.impala_query_info where day>=%s
              and day<=%s and query_type="DDL" and ddl_type='COMPUTE_STATS'
      """
}