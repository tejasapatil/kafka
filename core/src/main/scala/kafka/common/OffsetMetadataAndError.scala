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
package kafka.common

/**
 * A convenience case class that consolidates (offset, metadata, error) which is used in OffsetFetchResponse and OffsetCommitRequest
 * (although the 'error' field is only relevant for OffsetFetchResponse)
 */
case class OffsetMetadataAndError(offset: Long,
                                  timestamp: Long = OffsetAndMetadata.InvalidTime,
                                  metadata: String = OffsetAndMetadata.NoMetadata,
                                  error: Short = ErrorMapping.NoError) {

  def this(tuple: (Long, Long, String, Short)) =
    this(tuple._1, tuple._2, tuple._3, tuple._4)

  def this(offsetMetadata: OffsetAndMetadata, error: Short) =
    this(offsetMetadata.offset, offsetMetadata.timestamp, offsetMetadata.metadata, error)

  def this(error: Short) =
    this(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.InvalidTime, OffsetAndMetadata.NoMetadata, error)

  def asTuple = (offset, timestamp, metadata, error)

  override def toString = "OffsetMetadataAndError[%d,%d,%s,%d]".format(offset, timestamp, metadata, error)
}

object OffsetMetadataAndError {
  val NoError =
    OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.InvalidTime, OffsetAndMetadata.NoMetadata, ErrorMapping.NoError)
  val OffsetLoading =
    OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.InvalidTime, OffsetAndMetadata.NoMetadata, ErrorMapping.OffsetLoadingNotCompleteCode)
  val BrokerNotAvailable =
    OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.InvalidTime, OffsetAndMetadata.NoMetadata, ErrorMapping.BrokerNotAvailableCode)
  val UnknownTopicPartition =
    OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.InvalidTime, OffsetAndMetadata.NoMetadata, ErrorMapping.UnknownTopicOrPartitionCode)
}