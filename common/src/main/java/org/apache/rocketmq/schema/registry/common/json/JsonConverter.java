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

import org.apache.rocketmq.schema.registry.common.exception.SchemaException;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Json <-> Object
 */
public interface JsonConverter {

    /**
     * Parses the given string as json and returns a json ObjectNode representing the json.
     *
     * @param s a string representing a json object
     * @return an object node representation of the string
     * @throws SchemaException if unable to convert the string to json or the json isn't a json object.
     */
    ObjectNode fromJson(String s) throws SchemaException;

    /**
     * Parses the given JSON value.
     *
     * @param s     json string
     * @param clazz class
     * @param <T>   type of the class
     * @return object
     */
    <T> T fromJson(String s, Class<T> clazz);

    /**
     * Parses the given JSON value.
     *
     * @param s     json byte array
     * @param clazz class
     * @param <T>   type of the class
     * @return object
     */
    <T> T fromJson(byte[] s, Class<T> clazz);

    /**
     * Converts JSON as bytes.
     *
     * @param o object
     * @return byte array
     */
    byte[] toJsonAsBytes(Object o);

    /**
     * Converts an object to JSON.
     *
     * @param o object
     * @return JSON node
     */
    ObjectNode toJsonAsObjectNode(Object o);

    /**
     * Converts an object to JSON string.
     *
     * @param o object
     * @return JSON string
     */
    String toJson(Object o);

    /**
     * Converts an object to JSON string.
     *
     * @param o object
     * @return JSON string
     */
    String toString(Object o);

    /**
     * Converts String to bytes.
     *
     * @param s string array
     * @return byte array
     */
    byte[] toBytes(String s);
}
