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

package org.apache.rocketmq.schema.registry.common.filter;


import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

import org.apache.rocketmq.schema.registry.common.context.RequestContextManager;
import org.apache.rocketmq.schema.registry.common.context.RequestContext;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RequestFilter implements Filter {


    public RequestFilter() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        Filter.super.init(filterConfig);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void doFilter(
        ServletRequest request,
        ServletResponse response,
        FilterChain chain
    ) throws IOException, ServletException {
        // Pre-processing
        if (!(request instanceof HttpServletRequest)) {
            throw new ServletException("Expected an HttpServletRequest but didn't get one");
        }
        final HttpServletRequest httpServletRequest = (HttpServletRequest) request;
        final String method = httpServletRequest.getMethod();

        // TODO: get request Authorization from http header
//        final String requestAuth =
//            httpServletRequest.getHeader(RequestContext.MICOMPUTE_REQUEST_HEADER_AUTHORIZATION);
//        final String metaAccount = StringUtils.isNotBlank(requestAuth)
//            ? requestAuth.replaceAll("@<", "\\{").replaceAll("@>", "\\}")
//            : requestAuth;
        final RequestContext context = RequestContext.builder().build();
        RequestContextManager.putContext(context);
        log.info("filter " + context.toString());

        // Do the rest of the chain
        chain.doFilter(request, response);

        // Post processing
        RequestContextManager.removeContext();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        Filter.super.destroy();
    }
}
