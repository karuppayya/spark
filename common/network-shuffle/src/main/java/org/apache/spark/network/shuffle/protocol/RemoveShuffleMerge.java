/*
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

package org.apache.spark.network.shuffle.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.protocol.Encoders;

/**
 * Remove the merged data for a given shuffle.
 * Returns {@link Boolean}
 *
 * @since 3.4.0
 */
public class RemoveShuffleMerge extends BlockTransferMessage {
  public final String appId;
  public final int appAttemptId;
  public final int shuffleId;
  public final int shuffleMergeId;

  public RemoveShuffleMerge(
      String appId,
      int appAttemptId,
      int shuffleId,
      int shuffleMergeId) {
    this.appId = appId;
    this.appAttemptId = appAttemptId;
    this.shuffleId = shuffleId;
    this.shuffleMergeId = shuffleMergeId;
  }

  @Override
  protected Type type() {
    return Type.REMOVE_SHUFFLE_MERGE;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId, appAttemptId, shuffleId, shuffleMergeId);
  }

  @Override
  public String toString() {
    return "RemoveShuffleMerge[appId=" + appId + ",attemptId=" + appAttemptId +
        ",shuffleId=" + shuffleId + ",shuffleMergeId=" + shuffleMergeId + "]";
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof RemoveShuffleMerge o) {
      return Objects.equal(appId, o.appId)
        && appAttemptId == o.appAttemptId
        && shuffleId == o.shuffleId
        && shuffleMergeId == o.shuffleMergeId;
    }
    return false;
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId) + 4 + 4 + 4;
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    buf.writeInt(appAttemptId);
    buf.writeInt(shuffleId);
    buf.writeInt(shuffleMergeId);
  }

  public static RemoveShuffleMerge decode(ByteBuf buf) {
    String appId = Encoders.Strings.decode(buf);
    int attemptId = buf.readInt();
    int shuffleId = buf.readInt();
    int shuffleMergeId = buf.readInt();
    return new RemoveShuffleMerge(appId, attemptId, shuffleId, shuffleMergeId);
  }
}
