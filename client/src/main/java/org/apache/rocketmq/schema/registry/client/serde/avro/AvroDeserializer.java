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
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.config.AvroDeserializerConfig;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.client.serde.Deserializer;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class AvroDeserializer<T> implements Deserializer<T> {

    Logger log = LoggerFactory.getLogger(AvroDeserializer.class);

    private final DecoderFactory decoderFactory = DecoderFactory.get();
    protected SchemaRegistryClient schemaRegistry;

    private boolean useGenericReader;

    public AvroDeserializer(){}

    public AvroDeserializer(SchemaRegistryClient schemaRegistryClient) {
        schemaRegistry = schemaRegistryClient;
    }

    @Override
    public void configure(Map<String, Object> configs) {
        AvroDeserializerConfig config = new AvroDeserializerConfig(configs);
        this.useGenericReader = config.useGenericReader();
    }

    @Override
    public T deserialize(String subject, byte[] payload)
            throws SerializationException {
        if (schemaRegistry == null) {
            throw new SerializationException("please initialize the schema registry client first");
        }
        if (payload == null) {
            return null;
        }

        try {
            GetSchemaResponse response = schemaRegistry.getSchemaBySubject(subject);
            Schema schema = new Schema.Parser().parse(response.getIdl());
            return avroDecode(payload, schema);
        } catch (RestClientException | IOException e) {
            throw new RuntimeException(e);
        }

    }

    public T avroDecode(byte[] input, Schema schema) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(input);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bais, null);

        ByteBuffer buffer = ByteBuffer.allocate(16);
        try {
            decoder.readBytes(buffer);
        } catch (Exception e) {
            log.error("read bytes error: ", e);
        }

        long schemaRecordId = buffer.getLong();

        DatumReader<T> datumReader = getDatumReader(schema);
        T record = datumReader.read(null, decoder);
        return record;
    }

    private DatumReader<T> getDatumReader(Schema schema) {
        if (useGenericReader) {
            return new GenericDatumReader<>(schema);
        } else {
            return new SpecificDatumReader<>(schema);
        }
    }

    @Override
    public void close() {
    }
}
