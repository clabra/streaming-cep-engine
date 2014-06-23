package com.stratio.streaming.unit

import org.scalatest._
import org.scalatest.mock.MockitoSugar
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.mockito.Matchers._
import com.stratio.streaming.api.{ExecutionPlanScheduler, StreamingAPISyncOperation}
import com.stratio.streaming.commons.messages.StratioStreamingMessage

class ExecutionPlanSchedulerUnitTests
  extends FunSpec
  with GivenWhenThen
  with ShouldMatchers
  with MockitoSugar {

  val streamingAPISyncOperationMock = mock[StreamingAPISyncOperation]
  val executionPlanScheduler = new ExecutionPlanScheduler(streamingAPISyncOperationMock, "12345")


  describe("The execution plan scheduler") {
    it("should create a stream defined in the streaming engine snapshot") {
      val theStreamName = "unitTestsStream"
      Given("A streaming engine snapshot with a stream created")
      val streamingSnapshot = s"""{"streamName":"$theStreamName","columns":[{"column":"column1","type":"STRING"}],"queries":[],"activeActions":["INDEXED","SAVE_TO_CASSANDRA","LISTEN"],"userDefined":true}"""
      When("We call the execution plan scheduler")
      executionPlanScheduler.executePlan(streamingSnapshot)
      Then("We should create the stream")
      doNothing().when(streamingAPISyncOperationMock).performSyncOperation(anyObject[StratioStreamingMessage]())
      val arguments = ArgumentCaptor.forClass(classOf[StratioStreamingMessage])
      verify(streamingAPISyncOperationMock).performSyncOperation(arguments.capture)
      arguments.getValue.getStreamName() should be(theStreamName)
    }
  }

}
