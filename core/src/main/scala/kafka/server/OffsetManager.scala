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

import kafka.utils._
import kafka.log.{LogConfig, LogManager}
import kafka.common._
import kafka.message.Message
import java.nio.ByteBuffer
import kafka.producer.DefaultPartitioner
import java.util.Properties
import kafka.api.ApiUtils._

/**
 * Main interface for offset management which provides operations for storing, accessing, loading and
 * maintaining consumer offsets
 */
trait OffsetManager extends Logging {
  /**
   * Initializes the offset manager so that its ready to serve offsets
   */
  def startup()

  /**
   * Fetch the requested offset value from the underlying offsets storage
   *
   * @param key The requested group-topic-partition
   * @return if the offset is present, return it or else return None
   */
  def getOffset(key: GroupTopicPartition): OffsetMetadataAndError

  /**
   * Store the given key-offset value in the underlying offsets storage
   *
   * @param key The group-topic-partition
   * @param offset The offset to be stored
   */
  def putOffset(key: GroupTopicPartition, offset: OffsetAndMetadata)

  /**
   * Asynchronously load the offsets from logs into the offset manager
   *
   * @param partition The partition of the offsets topic which must be loaded
   */
  def syncOffsetsFromLogs(partition: Int)

  /**
   * Does cleanup and shutdown of any structures maintained by the offset manager
   */
  def shutdown()
}

object OffsetManager {
  val OffsetsTopicName = "__offsets"
  val Delimiter = "##"
  val OffsetsTopicProperties = new Properties()

  private var manager: OffsetManager = null
  private var offsetTopicPartitions = 1
  private val partitioner = new DefaultPartitioner
  private val lock = new Object

  /**
   * Creates the (singleton) offset manager.
   *
   * @param config Used to create a offset manager object based on "offset.storage" property
   * @return an offset manager
   */
  private def createOffsetManager(config: KafkaConfig, logManager: LogManager) {
    lock.synchronized {
      offsetTopicPartitions = config.offsetTopicPartitions
      OffsetsTopicProperties.put(LogConfig.SegmentBytesProp, config.offsetTopicSegmentBytes.toString)
      OffsetsTopicProperties.put(LogConfig.CleanupPolicyProp, "dedupe")

      config.offsetStorage.toLowerCase match {
        case "kafka"     => manager = new DefaultOffsetManager(config, logManager)
        case "zookeeper" => manager = new ZookeeperOffsetManager(config)
        case _  => throw new InvalidConfigException(("Invalid value of \"offset.storage\" : %s (specify one of " +
                                                     "\"kafka\" or \"zookeeper\")").format(config.offsetStorage))
      }
    }
  }

  /**
    * Implements singleton pattern to allow only one instance of the offset manager in every context.
    *
    * @param config Used to create a offset manager object if not present
    * @return an offset manager object associated with the current context
    */
  def getOffsetManager(config: KafkaConfig, logManager: LogManager) : OffsetManager = {
    if(manager == null)
      createOffsetManager(config, logManager)
    manager
  }

  /**
   * Gives the offsets' topic partition responsible for the given consumer group id
   *
   * @param group consumer group id
   * @return offsets' topic partition
   */
  def partitionFor(group: String): Int = partitioner.partition(group, offsetTopicPartitions)

  /**
   * Generates the key for offset commit message for given (group, topic, partition)
   *
   * @return key for offset commit message
   */
  def offsetCommitKey(group: String, topic: String, partition: Int, versionId: Short = 0): Array[Byte] = {
    val length = 2 + /* versionId */
                 shortStringLength(group) +
                 shortStringLength(topic) +
                 4  /* partition */

    val byteBuffer = ByteBuffer.allocate(length)
    byteBuffer.putShort(versionId)
    writeShortString(byteBuffer, group)
    writeShortString(byteBuffer, topic)
    byteBuffer.putInt(partition)

    byteBuffer.array()
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an GroupTopicPartition object
   */
  def readMessageKey(buffer: ByteBuffer): GroupTopicPartition = {
    val versionId = buffer.getShort     // currently not being used
    val group = readShortString(buffer)
    val topic = readShortString(buffer)
    val partition = buffer.getInt
    GroupTopicPartition(group, TopicAndPartition(topic, partition))
  }

  /**
   * Generates the payload for offset commit message from given offset and metadata
   *
   * @param offset input offset
   * @param metadata metadata
   * @return payload for offset commit message
   */
  def offsetCommitValue(offset: Long, metadata: String = OffsetAndMetadata.NoMetadata): Array[Byte] = {
    val length = 8 + 8 + shortStringLength(metadata) + 8

    val byteBuffer = ByteBuffer.allocate(length)
    byteBuffer.putLong(offset)
    byteBuffer.putLong(SystemTime.milliseconds)
    writeShortString(byteBuffer, metadata)
    byteBuffer.array()
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  def readMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    if(buffer == null) {
      null
    } else {
      val offset = buffer.getLong
      val timestamp = buffer.getLong
      val metadata = readShortString(buffer)
      OffsetAndMetadata(offset,timestamp,metadata)
    }
  }

  /**
   * A convenience case class for key-value maintained by offset manager
   * Provides secondary constructor which is useful to extract and decode the key-value pair from a kakfa message
   */
  case class KeyValue(key: GroupTopicPartition, value: OffsetAndMetadata) {
    def this(message: Message) = this(readMessageKey(message.key), readMessageValue(message.payload))
    override def toString = "[%s-%s]".format(key.toString, if(value != null) value.toString else "null")
    def asTuple = (key, value)
  }
}
