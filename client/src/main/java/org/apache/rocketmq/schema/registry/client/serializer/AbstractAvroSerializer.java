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

package org.apache.rocketmq.schema.registry.client.serializer;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.common.dto.SchemaRecordDto;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

public class AbstractAvroSerializer<T> {

    private static final int SCHEMA_ID_LENGTH = 16;
    protected SchemaRegistryClient schemaRegistry;
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    protected byte[] serializeImpl(
            String subject, T originMessage)
            throws SerializationException {
        if (schemaRegistry == null) {
            throw new SerializationException("please initialize the schema registry client first");
        }

        if (originMessage == null) {
            return null;
        }

        try {

        } catch (Exception e) {
            throw new SerializationException("get schema by subject failed", e);
        }

        try {
            SchemaRecordDto schemaRecordDto = getSchemaBySubject(subject);
            long schemaId = schemaRecordDto.getSchemaId();
            String schemaIdl = schemaRecordDto.getIdl();
            Schema schema = new Schema.Parser().parse(schemaIdl);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(ByteBuffer.allocate(SCHEMA_ID_LENGTH).putLong(schemaId).array());
            writeDatum(out, originMessage, schema);

            byte[] bytes = out.toByteArray();
            out.close();
            return bytes;
        } catch (ExecutionException e) {
            throw new SerializationException("serialize Avro message failed", e.getCause());
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("serialize Avro message failed", e);
        } catch (RestClientException e) {
            throw new SerializationException("get schema by subject failed", e);
        }
    }

    private void writeDatum(ByteArrayOutputStream out, Object originMessage, Schema schema)
            throws ExecutionException, IOException {
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);

        DatumWriter<Object> datumWriter = new SpecificDatumWriter<>(schema);
        datumWriter.write(originMessage, encoder);
        encoder.flush();
        datumWriter.write(originMessage, encoder);
        encoder.flush();
    }

    private SchemaRecordDto getSchemaBySubject(String subject) throws RestClientException, IOException {
        return schemaRegistry.getSchemaBySubject(subject);
    }

}
