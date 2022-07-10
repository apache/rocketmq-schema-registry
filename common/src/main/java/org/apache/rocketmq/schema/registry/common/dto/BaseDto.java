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
package org.apache.rocketmq.schema.registry.common.dto;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.schema.registry.common.json.JsonConverter;
import org.apache.rocketmq.schema.registry.common.json.JsonConverterImpl;


/**
 * Base class for all DTOs, and all DTOs should be READ-ONLY.
 */
public abstract class BaseDto implements Serializable {
    protected static final JsonConverter JSON_CONVERTER = new JsonConverterImpl();

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return JSON_CONVERTER.toString(this);
    }
//
//    /**
//     * Deserialize data from the input stream.
//     *
//     * @param inputStream input stream
//     * @return Json ObjectNode
//     * @throws IOException exception deserializing the stream
//     */
//    @Nullable
//    public static ObjectNode deserializeObjectNode(
//        @Nonnull @NonNull final ObjectInputStream inputStream
//    ) throws IOException {
//        return JSON_CONVERTER.deserializeObjectNode(inputStream);
//    }
//
//    /**
//     * Serialize data in the output stream.
//     *
//     * @param outputStream output stream
//     * @param jsonObject         jsonObject
//     * @throws IOException exception serializing the json
//     */
//    public static void serializeObjectNode(
//        @Nonnull @NonNull final ObjectOutputStream outputStream,
//        @Nullable final ObjectNode jsonObject
//    ) throws IOException {
//        JSON_CONVERTER.serializeObjectNode(outputStream, jsonObject);
//    }
}
