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

package org.apache.rocketmq.schema.registry.common.utils;

import com.google.common.base.Preconditions;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;
import org.apache.rocketmq.schema.registry.common.properties.GlobalConfig;

public class SnowFlakeIdGenerator implements IdGenerator {

    // machine node id [0-63]
    private final long nodeId;
    // region id [0-15]
    private final long regionId;
    // sequence id [0-4095]
    private long sequenceId = 0L;
    private long lastTimestamp = -1L;

    // startTime, UTC 2020-01-01 00:00:00
    private final long startTime = 1577808000000L;

    // the using bits number by region id of master-role cluster and machine-node id
    private final long regionIdBits = 4L;
    private final long nodeIdBits = 6L;
    // max machine-node id: 63
    private final long maxNodeId = ~(-1 << nodeIdBits);
    // max region id: 15
    private final long maxRegionId = ~(-1 << regionIdBits);

    // the bits number of sequenceId used
    private final long sequenceIdBits = 12L;
    // the bits number of nodeId left moving, 12bits
    private final long nodeIdMoveBits = sequenceIdBits;
    // the bits number of regionId left moving, 18bits
    private final long regionIdMoveBits = nodeIdMoveBits + nodeIdBits;
    // the bits number of timestamp left moving, 22bits
    private final long timestampMoveBits = regionIdMoveBits + regionIdBits;

    // the mask of sequenceId, 4095
    private final long sequenceIdMask = ~(-1L << sequenceIdBits);

    public SnowFlakeIdGenerator(GlobalConfig config) {
        this.regionId = config.getServiceRegionId();
        this.nodeId = config.getServiceNodeId();

        Preconditions.checkArgument(nodeId <= maxNodeId && nodeId >= 0,
            "The NodeId can not be greater than %d or less than 0", maxNodeId);
        Preconditions.checkArgument(regionId <= maxRegionId && regionId >= 0,
            "The RegionId can not be greater than %d or less than 0", maxRegionId);
    }

    @Override
    public synchronized long nextId() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            throw new SchemaException("SchemaId generating error, clock moved backwards, please try later");
        }

        // if timestamp equals to lastTimestamp, diff the sequenceId
        if (lastTimestamp == timestamp) {
            // prevent sequenceId greater than 4095 (number of 'sequenceIdBits')
            sequenceId = (sequenceId + 1) & sequenceIdMask;
            // if sequenceId equals to 0, it means that
            // the 'sequenceIdBits' has been exhausted at the current timestamp, then
            // it would be blocked for a new timestamp
            if (sequenceId == 0) {
                timestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            sequenceId = 0L;
        }

        lastTimestamp = timestamp;

        // computing the 64 bits unique id
        return ((timestamp - startTime) << timestampMoveBits)
            | (regionId << regionIdMoveBits)
            | (nodeId << nodeIdMoveBits)
            | sequenceId;
    }

    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
