package com.barrelsofdata.sparkexamples

import java.sql.Timestamp

import org.apache.spark.sql.streaming.GroupState

import scala.math.Ordering

object StateOperations {

  implicit def ordered: Ordering[Timestamp] = (x: Timestamp, y: Timestamp) => x compareTo y

  def deduplicate(user: String,
                  currentBatchData: Iterator[UserData],
                  state: GroupState[UserData]): Iterator[UserData] = {

    var currentState: Option[UserData] = state.getOption

    val sortedBatch = currentBatchData.toSeq.sortBy(_.eventTime)

    val results = for {
      userData <- sortedBatch
      if currentState.isEmpty || currentState.get.temperature != userData.temperature
    } yield {
      currentState = Some(userData)
      userData
    }
    state.update(currentState.get)
    results.toIterator
  }

}
