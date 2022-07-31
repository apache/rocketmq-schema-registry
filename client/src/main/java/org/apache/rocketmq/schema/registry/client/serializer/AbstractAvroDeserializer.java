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
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.exceptions.SerializationException;
import org.apache.rocketmq.schema.registry.common.dto.GetSchemaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AbstractAvroDeserializer<T> {

    Logger log = LoggerFactory.getLogger(AvroDeserializer.class);

    private final DecoderFactory decoderFactory = DecoderFactory.get();
    protected SchemaRegistryClient schemaRegistry;

    protected T deserializeImpl(String subject, byte[] payload)
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

        long id = buffer.getLong();
        long version = buffer.getLong();

        DatumReader<T> datumReader = new SpecificDatumReader<T>(schema);
        T originMessage = datumReader.read(null, decoder);
        return originMessage;
    }

}
