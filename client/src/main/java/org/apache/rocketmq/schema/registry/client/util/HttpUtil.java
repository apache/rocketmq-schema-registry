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

package org.apache.rocketmq.schema.registry.client.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class HttpUtil {
    public static ObjectMapper jsonParser = JacksonMapper.INSTANCE;
    private static final int HTTP_CONNECT_TIMEOUT_MS = 30000;
    private static final int HTTP_READ_TIMEOUT_MS = 30000;
    private static final int ERROR_CODE = 5001;

    public static <T> T sendHttpRequest(String requestUrl, String method, String requestBodyData,
                                        Map<String, String> requestProperties,
                                        TypeReference<T> responseFormat)
            throws IOException, RestClientException {

        HttpURLConnection connection = null;
        try {
            URL url = new URL(requestUrl);

            connection = buildConnection(url, method, requestProperties);

            if (requestBodyData != null) {
                connection.setDoOutput(true);
                try (DataOutputStream os = new DataOutputStream(connection.getOutputStream())) {
                    os.writeBytes(requestBodyData);
                    os.flush();
                } catch (IOException e) {
                    throw e;
                }
            }

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK || responseCode == HttpURLConnection.HTTP_CREATED) {
                InputStream is = connection.getInputStream();
                T result = jsonParser.readValue(is, responseFormat);
                is.close();
                return result;
            } else if (responseCode == HttpURLConnection.HTTP_NO_CONTENT) {
                return null;
            } else {
                throw new RestClientException("send request failed", responseCode,
                        ERROR_CODE);
            }

        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private static HttpURLConnection buildConnection(URL url, String method, Map<String,
            String> requestProperties)
            throws IOException {
        HttpURLConnection connection = null;
        connection = (HttpURLConnection) url.openConnection();

        connection.setConnectTimeout(HTTP_CONNECT_TIMEOUT_MS);
        connection.setReadTimeout(HTTP_READ_TIMEOUT_MS);

        connection.setRequestMethod(method);
        connection.setDoInput(true);

        for (Map.Entry<String, String> entry : requestProperties.entrySet()) {
            connection.setRequestProperty(entry.getKey(), entry.getValue());
        }

        connection.setUseCaches(false);

        return connection;
    }

    public static String buildRequestUrl(String baseUrl, String path) {
        return baseUrl.replaceFirst("/$", "") + "/" + path.replaceFirst("^/", "");
    }
}
