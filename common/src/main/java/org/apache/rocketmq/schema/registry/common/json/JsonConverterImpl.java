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

package org.apache.rocketmq.schema.registry.common.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.schema.registry.common.exception.SchemaException;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.Getter;

@NoArgsConstructor
@Getter
@Slf4j
public class JsonConverterImpl implements JsonConverter {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Gson gson = new Gson();

    @Override
    public ObjectNode fromJson(String s) throws SchemaException {
        return null;
    }

    @Override
    public <T> T fromJson(String s, Class<T> clazz) {
        return gson.fromJson(s, clazz);
//        T dst;
//        try {
//            dst = mapper.readValue(s, clazz);
//        } catch (Exception e){
//            throw new SchemaException("Parse data failed", e);
//        }
//        return dst;
    }

    @Override
    public <T> T fromJson(byte[] s, Class<T> clazz) {
        return gson.fromJson(new String(s), clazz);
//        T dst;
//        try {
//            dst = mapper.readValue(s, clazz);
//        } catch (Exception e){
//            throw new SchemaException("Parse data failed", e);
//        }
//        return dst;
    }

    @Override
    public byte[] toJsonAsBytes(Object o) {
        return gson.toJson(o).getBytes(StandardCharsets.UTF_8);
//        byte[] dst;
//        try {
//            dst = mapper.writeValueAsBytes(o);
//        } catch (Exception e){
//            throw new SchemaException("Parse data failed", e);
//        }
//        return dst;
    }

    @Override
    public ObjectNode toJsonAsObjectNode(Object o) {
        return null;
    }

    @Override
    public String toJson(Object o) {
        return gson.toJson(o);
    }

    @Override
    public String toString(Object o) {
        return null;
    }

    @Override
    public byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
