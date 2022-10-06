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
package org.apache.rocketmq.schema.registry.client.serde.avro;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.rocketmq.schema.registry.client.config.AvroSerdeConfig;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.serde.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

public class ReflectionAvroDeserializer<T> implements Deserializer<T> {
    private Type type;

    public ReflectionAvroDeserializer() {
    }

    @Override
    public void configure(Map<String, Object> configs) {
        AvroSerdeConfig avroSerdeConfig = new AvroSerdeConfig(configs);
        this.type = avroSerdeConfig.deserializeTargetType();
    }

    @Override
    public T deserialize(String subject, byte[] bytes) {
        if (null == bytes) return null;

        if (null == type) {
            throw new SerializationException("deserialize type can not be null");
        }
        Schema schema = ReflectData.get().getSchema(type);
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bais, null);
            DatumReader<T> datumReader = new ReflectDatumReader<>(schema);
            T record = datumReader.read(null, decoder);
            return record;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
