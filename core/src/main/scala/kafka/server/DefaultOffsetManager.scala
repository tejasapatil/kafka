/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.log.LogManager
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.{SystemTime, KafkaScheduler, Pool}
import kafka.common._
import java.util.concurrent.ConcurrentSkipListSet
import com.yammer.metrics.core.Gauge
import scala.Some
import kafka.common.GroupTopicPartition
import kafka.message.MessageAndOffset

/**
 * An inbuilt table based implementation of the offset manager trait
 */
private class DefaultOffsetManager(val config: KafkaConfig,
                                   val logManager: LogManager) extends OffsetManager with KafkaMetricsGroup {

  /* This in-memory hash table would store the consumer offsets */
  private val offsetsTable = new Pool[GroupTopicPartition, OffsetAndMetadata]

  /* A set of partitions of offsets' topic which the offset manager is currently loading */
  private val loading = new ConcurrentSkipListSet[Int]

  /* Thread-pool for performing the asynchronous loading. If a broker, leading 'X' partitions of the offsets' topic, fails
   * then the ownership of those partitions would be distributed across the remaining brokers. The #partitions to be
   * loaded per broker would be less so we are creating a thread pool of just 5 threads */
  private val scheduler = new KafkaScheduler(5, "offsets-table-loader-")

  /* While reading messages from disk, read 10 MB at a time */
  private val readBatchSize = 10*1024*1024

  /* Lock used to prevent loading process from replacing new offsets being committed */
  private val loadLock = new Object

  newGauge("DefaultOffsetManager-NumOffsets",
    new Gauge[Int] {
      def value = offsetsTable.size
    })

  newGauge("DefaultOffsetManager-NumConsumerGroups",
    new Gauge[Int] {
      def value = offsetsTable.keys.toSeq.map(_.group).toSet.size
    })

  def startup() {
    info("Starting default offset manager")
    try {
      scheduler.startup()
    } catch {
      case e: IllegalStateException =>
        warn("Scheduler was already running.")
    }
  }

  /**
   * Checks if the requested offset partition is currently being loaded and if so would return a LoadingOffset code
   * else would return the offset value from the offset table. Returns "None" if no value is found in the offset table.
   */
  def getOffset(key: GroupTopicPartition): OffsetMetadataAndError = {
    val offsetMetadataError = loadLock synchronized {
      if(loading.contains(OffsetManager.partitionFor(key.group)))
        OffsetMetadataAndError.OffsetLoading
      else {
        val offsetAndMetadata = offsetsTable.get(key)
        if (offsetAndMetadata == null)
          OffsetMetadataAndError.UnknownTopicPartition
        else
          OffsetMetadataAndError(offsetAndMetadata.offset, offsetAndMetadata.timestamp, offsetAndMetadata.metadata, ErrorMapping.NoError)
      }
    }

    debug("Fetched offset %s for key %s".format(offsetMetadataError, key))
    offsetMetadataError
  }

  /**
   * Adds the key-offset pair to the offsets table. If the requested offset partition is currently
   * being loaded then it would be added to 'commitsWhileLoading' so that loading won't override it.
   */
  def putOffset(key: GroupTopicPartition, offset: OffsetAndMetadata) = {
    if(loading.contains(OffsetManager.partitionFor(key.group)))
      loadLock synchronized {
        if (offset != null)
          offsetsTable.put(key,offset)
        else
          offsetsTable.remove(key)
      }
    else {
      if (offset != null)
        offsetsTable.put(key,offset)
      else
        offsetsTable.remove(key)
    }

    debug("Added offset %s for key %s".format(if(offset != null) offset else "null", key))
  }

  /**
   * Asynchronously read the messages for offsets topic from the logs and populate the hash table of offsets.
   */
  def syncOffsetsFromLogs(offsetsPartition: Int) {
    loading.add(offsetsPartition)   // prevent any offset fetch directed to this partition of the offsets topic
    scheduler.schedule("[%s,%d]".format(OffsetManager.OffsetsTopicName, offsetsPartition), loadOffsets)

    def loadOffsets() {
      val startTime = SystemTime.milliseconds
      val topicPartition = TopicAndPartition(OffsetManager.OffsetsTopicName, offsetsPartition)

      try {
        logManager.getLog(topicPartition) match {
          case Some(log) =>
            var currOffset = log.logSegments.head.baseOffset

            while(currOffset < log.logEndOffset) {
              log.read(currOffset, readBatchSize).iterator.foreach {
                case m: MessageAndOffset =>
                  val record = new OffsetManager.KeyValue(m.message)
                  loadLock synchronized {
                    val previousOffset = offsetsTable.get(record.key)
                    if(previousOffset == null || record.value.timestamp > previousOffset.timestamp)
                      offsetsTable.put(record.key, record.value)
                  }
                  currOffset = m.nextOffset
              }
            }

            info("Loading offsets for %s completed in %d ms".format(topicPartition, SystemTime.milliseconds - startTime))
          case None =>
            warn("No log found for " + topicPartition)
        }
      } catch {
        case t: Throwable =>
          error("Error in loading offsets from " + topicPartition, t)
      } finally {
        loading.remove(offsetsPartition)  // resume offset fetches directed to this partition of the offsets topic
      }
    }
  }

  def shutdown() {
    info("Shutting down.")
    try {
      scheduler.shutdown()
    } catch {
      case e: Exception =>                       // indicates that scheduler was already shutdown during previous attempt
        debug("Scheduler was already shutdown.") // to shutdown offset manager. Log the exception and let it go
    }
  }
}