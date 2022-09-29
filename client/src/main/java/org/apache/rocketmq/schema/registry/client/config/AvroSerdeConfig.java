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
package org.apache.rocketmq.schema.registry.client.config;

import java.util.Map;

public class AvroSerdeConfig extends SerdeConfig {
    /**
     * use generic datum reader to deserialize genericRecord.
     */
    public final static String USE_GENERIC_DATUM_READER =
        "use.generic.datum.reader";
    public final static boolean USE_GENERIC_DATUM_READER_DEFAULT = false;

    public AvroSerdeConfig(Map<String, Object> configs) {
        this.configs = configs;
    }

    public boolean useGenericReader() {
        return (boolean) configs.getOrDefault(USE_GENERIC_DATUM_READER, USE_GENERIC_DATUM_READER_DEFAULT);
    }
}
