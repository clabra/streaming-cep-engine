package com.stratio.streaming.api

import com.google.gson.Gson
import com.stratio.streaming.commons.streams.StratioStream
import com.stratio.streaming.commons.constants.STREAM_OPERATIONS
import com.stratio.streaming.messaging.{ColumnNameType, MessageBuilderWithColumns}
import scala.collection.JavaConversions._
import java.util

class ExecutionPlanScheduler(syncOperation: StreamingAPISyncOperation, sessionId: String) {
  def executePlan(streamingSnapshot: String) {
     val streams = new Gson().fromJson(streamingSnapshot, classOf[java.util.List[StratioStream]])
     createStreams(streams.toList)
   }
  
  def createStreams(streams: List[StratioStream]) {
    streams.foreach(stream => syncOperation.performSyncOperation(stratioStreamToStratioStreamingMessage(stream, STREAM_OPERATIONS.DEFINITION.CREATE.toLowerCase)))
  }

  def stratioStreamToStratioStreamingMessage(stratioStream: StratioStream, operation: String) = {
    val columnNameTypeList = stratioStream.getColumns.map(element => new ColumnNameType(element.getColumn, element.getType))
    MessageBuilderWithColumns(sessionId, operation).
      build(stratioStream.getStreamName, columnNameTypeList)
  }
}
