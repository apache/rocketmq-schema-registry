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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.rocketmq.schema.registry.client.exceptions.RestClientException;
import org.apache.rocketmq.schema.registry.client.rest.JacksonMapper;
import org.apache.rocketmq.schema.registry.common.utils.ErrorMessage;

public class HttpUtil {
    public static ObjectMapper jsonParser = JacksonMapper.INSTANCE;
    private static final int HTTP_CONNECT_TIMEOUT_MS = 30000;
    private static final int HTTP_READ_TIMEOUT_MS = 30000;
    private static final int PARSE_ERROR_CODE = 5001;

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
                try (OutputStream os = connection.getOutputStream()) {
                    os.write(requestBodyData.getBytes(StandardCharsets.UTF_8));
                    os.flush();
                } catch (IOException e) {
                    throw e;
                }
            }

            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                InputStream is = connection.getInputStream();
                T result = jsonParser.readValue(is, responseFormat);
                is.close();
                return result;
            } else {
                ErrorMessage errorMessage;
                try (InputStream es = connection.getErrorStream()) {
                    if (es != null) {
                        errorMessage = jsonParser.readValue(es, ErrorMessage.class);
                    } else {
                        errorMessage = new ErrorMessage(PARSE_ERROR_CODE, "request error");
                    }
                } catch (JsonProcessingException e) {
                    errorMessage = new ErrorMessage(PARSE_ERROR_CODE, e.getMessage());
                }

                throw new RestClientException(errorMessage.getStatus(), errorMessage.getMessage());
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
