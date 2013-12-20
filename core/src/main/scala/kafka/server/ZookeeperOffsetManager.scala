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

import org.I0Itec.zkclient.ZkClient
import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, GroupTopicPartition}
import kafka.utils.{ZkUtils, ZKStringSerializer, ZKGroupTopicDirs}

/**
 * A Zookeeper based implementation of the offset manager trait
 */
private class ZookeeperOffsetManager(val config: KafkaConfig) extends OffsetManager {

  private var zkClient: ZkClient = null

  /* A regex pattern to unpack OffsetAndMetadata from a delimited string */
  val zkEntry = ("""(\d+)""" + OffsetManager.Delimiter +  // consumer offset value
    """(\d+)""" + OffsetManager.Delimiter +  // timestamp
    """(.*)""").r                            // metadata

  private def getPartitionDir(key: GroupTopicPartition): String =
    new ZKGroupTopicDirs(key.group, key.topicPartition.topic).consumerOffsetDir + "/" + key.topicPartition.partition

  def startup() {
    zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
  }

  def getOffset(key: GroupTopicPartition): OffsetMetadataAndError = {
    val offsetMetadataError = ZkUtils.readDataMaybeNull(zkClient, getPartitionDir(key))._1 match {
      case Some(value) =>
        val zkEntry(offset, timestamp, metadata) = value
        OffsetMetadataAndError(offset.toLong, timestamp.toLong, metadata)
      case None =>
        OffsetMetadataAndError.UnknownTopicPartition
    }
    debug("Fetched offset %s for key %s".format(offsetMetadataError, key))
    offsetMetadataError
  }

  def putOffset(key: GroupTopicPartition, value: OffsetAndMetadata) = {
    if (value != null) {
      val offsetData = value.offset.toString + OffsetManager.Delimiter + value.timestamp + OffsetManager.Delimiter + value.metadata
      ZkUtils.updatePersistentPath(zkClient, getPartitionDir(key), offsetData)
    } else {
      ZkUtils.deletePathRecursive(zkClient, getPartitionDir(key))
    }

    debug("Added offset %s for key %s".format(if(value != null) value else "null", key))
  }

  def syncOffsetsFromLogs(offsetsPartition: Int) {}

  def shutdown() {
    info("shutting down")
  }
}
