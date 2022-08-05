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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.serde.Serializer;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class AvroSerializer<T> implements Serializer<T> {

    private static final int SCHEMA_ID_LENGTH = 8;
    private static final int SCHEMA_VERSION_LENGTH = 8;
    protected SchemaRegistryClient schemaRegistry;
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    public AvroSerializer() {}

    public AvroSerializer(SchemaRegistryClient schemaRegistryClient) {
        schemaRegistry = schemaRegistryClient;
    }

    @Override
    public void configure(Map<String, Object> configs) {
        Serializer.super.configure(configs);
    }

    @Override
    public byte[] serialize(
            String subject, T record)
            throws SerializationException {
        if (schemaRegistry == null) {
            throw new SerializationException("please initialize the schema registry client first");
        }

        if (record == null) {
            return null;
        }

        try {
            GetSchemaResponse response = getSchemaBySubject(subject);
            long schemaRecordId = response.getRecordId();
            String schemaIdl = response.getIdl();
            Schema schema = new Schema.Parser().parse(schemaIdl);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            ByteBuffer buffer = ByteBuffer.allocate(SCHEMA_ID_LENGTH + SCHEMA_VERSION_LENGTH);
            encoder.writeBytes(buffer.putLong(schemaRecordId).array());

            DatumWriter<T> datumWriter;
            if (record instanceof SpecificRecord) {
                datumWriter = new SpecificDatumWriter<>(schema);
            } else {
                datumWriter = new GenericDatumWriter<>(schema);
            }
            datumWriter.write(record, encoder);
            encoder.flush();
            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("serialize Avro message failed", e);
        } catch (RestClientException e) {
            throw new SerializationException("get schema by subject failed", e);
        }
    }

    private GetSchemaResponse getSchemaBySubject(String subject) throws RestClientException, IOException {
        return schemaRegistry.getSchemaBySubject(subject);
    }

    @Override
    public void close() {
    }
}
