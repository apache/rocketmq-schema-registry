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

/**
 * service limit:
 *
 * Expire time: 69 year
 * Region number: 8
 * Node number: 16
 * Request number that can be processed in uniform milliseconds: 2
 * Record number: 16384
 *
 * 63 bit: default 0
 * 62-22 bit: expire time
 * 21-19 bit: regionId
 * 18-15 bit: nodeId
 * 14    bit: sequenceId
 * 13-0  bit: recordId
 */
public class SnowFlakeIdGenerator implements IdGenerator {

    // region id [0-7]
    private final long regionId;
    // machine node id [0-15]
    private final long nodeId;
    // sequence id [0-1]
    private long sequenceId = 0L;
    private long lastTimestamp = -1L;

    // startTime, UTC 2022-01-01 00:00:00
    // TODO: reset time after the limit is exceeded
    private final long startTime = 1640966400000L;

    // the using bits number by region id of master-role cluster and machine-node id
    private final long regionIdBits = 3L;
    private final long nodeIdBits = 4L;
    // max region id: 7
    private final long maxRegionId = ~(-1 << regionIdBits);
    // max machine-node id: 15
    private final long maxNodeId = ~(-1 << nodeIdBits);

    // the bits number of sequenceId used
    private final long sequenceIdBits = 1L;
    // the bits number of recordId used, [0-16383]
    private final long recordIdBits = 14L;

    private final long sequenceIdMoveBits = recordIdBits;
    // the bits number of nodeId left moving, 15bits
    private final long nodeIdMoveBits = sequenceIdBits + recordIdBits;
    // the bits number of regionId left moving, 18bits
    private final long regionIdMoveBits = nodeIdMoveBits + nodeIdBits;
    // the bits number of timestamp left moving, 22bits
    private final long timestampMoveBits = regionIdMoveBits + regionIdBits;

    // the mask of sequenceId, 1
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
            | (sequenceId << sequenceIdMoveBits);
    }

    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
