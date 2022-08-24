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
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.serde.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class ReflectionAvroSerializer<T> implements Serializer<T> {
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    public ReflectionAvroSerializer() {
    }

    @Override
    public void configure(Map<String, Object> configs) {

    }

    @Override
    public byte[] serialize(String subject, T record) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            Schema schema = ReflectData.get().getSchema(record.getClass());

            DatumWriter<T> datumWriter = new ReflectDatumWriter<>(schema);
            datumWriter.write(record, encoder);
            encoder.flush();
            byte[] bytes = out.toByteArray();
            return bytes;
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("serialize Avro message failed", e);
        }
    }

    @Override
    public void close() {
    }
}
