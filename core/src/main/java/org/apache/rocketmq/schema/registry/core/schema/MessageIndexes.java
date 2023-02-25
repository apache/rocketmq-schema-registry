/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.schema.registry.core.schema;

import org.apache.rocketmq.schema.registry.core.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MessageIndexes {

  private static final List<Integer> DEFAULT_INDEX = Collections.singletonList(0);

  private List<Integer> indexes;

  public MessageIndexes(List<Integer> indexes) {
    this.indexes = indexes;
  }

  public List<Integer> indexes() {
    return indexes;
  }

  public byte[] toByteArray() {
    if (indexes.equals(DEFAULT_INDEX)) {
      // optimization
      ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(0));
      ByteUtils.writeVarint(0, buffer);
      return buffer.array();
    }
    int size = ByteUtils.sizeOfVarint(indexes.size());
    for (Integer index : indexes) {
      size += ByteUtils.sizeOfVarint(index);
    }
    ByteBuffer buffer = ByteBuffer.allocate(size);
    writeTo(buffer);
    return buffer.array();
  }

  public void writeTo(ByteBuffer buffer) {
    ByteUtils.writeVarint(indexes.size(), buffer);
    for (Integer index : indexes) {
      ByteUtils.writeVarint(index, buffer);
    }
  }

  public static MessageIndexes readFrom(byte[] bytes) {
    return readFrom(ByteBuffer.wrap(bytes));
  }

  public static MessageIndexes readFrom(ByteBuffer buffer) {
    int size = ByteUtils.readVarint(buffer);
    if (size == 0) {
      // optimization
      return new MessageIndexes(DEFAULT_INDEX);
    }
    List<Integer> indexes = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      indexes.add(ByteUtils.readVarint(buffer));
    }
    return new MessageIndexes(indexes);
  }
}
