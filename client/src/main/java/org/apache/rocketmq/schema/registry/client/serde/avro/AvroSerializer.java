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
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.serde.Serializer;
import org.apache.rocketmq.schema.registry.common.constant.SchemaConstants;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class AvroSerializer<T> implements Serializer<T> {

    protected SchemaRegistryClient schemaRegistry;
    private final EncoderFactory encoderFactory = EncoderFactory.get();

    public AvroSerializer() {}

    public AvroSerializer(SchemaRegistryClient schemaRegistryClient) {
        schemaRegistry = schemaRegistryClient;
    }

    @Override
    public void configure(Map<String, Object> configs) {
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
        String purposeSchema;
        if (record instanceof GenericRecord) {
            purposeSchema = ((GenericContainer) record).getSchema().toString();
        } else {
            purposeSchema = SpecificData.get().getSchema(record.getClass()).toString();
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            GetSchemaResponse response = schemaRegistry.getTargetSchema(subject, purposeSchema);
            if (response == null) {
                throw new SerializationException("there's no corresponding schema version equals to given schema : " + purposeSchema);
            }
            long schemaRecordId = response.getRecordId();
            String schemaIdl = response.getIdl();
            Schema schema = new Schema.Parser().parse(schemaIdl);
            ByteBuffer buffer = ByteBuffer.allocate(SchemaConstants.SCHEMA_RECORD_ID_LENGTH);
            encoder.writeBytes(buffer.putLong(schemaRecordId).array());

            DatumWriter<T> datumWriter;
            if (record instanceof SpecificRecord) {
                datumWriter = new SpecificDatumWriter<>(schema);
            } else {
                datumWriter = new GenericDatumWriter<>(schema);
            }
            datumWriter.write(record, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException | RuntimeException e) {
            throw new SerializationException("serialize Avro message failed", e);
        } catch (RestClientException e) {
            throw new SerializationException("get target schema failed", e);
        }
    }

    @Override
    public void close() {
    }
}
