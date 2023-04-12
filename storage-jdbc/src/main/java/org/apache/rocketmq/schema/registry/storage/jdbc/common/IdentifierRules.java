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
package org.apache.rocketmq.schema.registry.storage.jdbc.common;

/**
 * The rules for how identifiers are parsed and quoted.
 */
public class IdentifierRules {
    public static final String DEFAULT_QUOTE = "\"";
    public static final String DEFAULT_ID_DELIM = ".";

    public static final IdentifierRules DEFAULT = new IdentifierRules(DEFAULT_ID_DELIM,
            DEFAULT_QUOTE
    );

    private final String leadingQuoteString;
    private final String trailingQuoteString;
    private final String identifierDelimiter;

    /**
     * Create new identifier rules using the supplied quote string for both leading and trailing
     * quotes, and the '{@link #DEFAULT_ID_DELIM}' character for identifier delimiters.
     *
     * @param quoteString the string used for leading and trailing quotes; may be null if {@link
     *                    #DEFAULT_QUOTE} is to be used
     */
    public IdentifierRules(String quoteString) {
        this(DEFAULT_ID_DELIM, quoteString, quoteString);
    }

    /**
     * Create new identifier rules using the supplied parameters.
     *
     * @param delimiter   the delimiter used within fully qualified names; may be null if {@link
     *                    #DEFAULT_ID_DELIM} is to be used
     * @param quoteString the string used for leading and trailing quotes; may be null if {@link
     *                    #DEFAULT_QUOTE} is to be used
     */
    public IdentifierRules(
            String delimiter,
            String quoteString
    ) {
        this(delimiter, quoteString, quoteString);
    }

    /**
     * Create new identifier rules using the supplied parameters.
     *
     * @param identifierDelimiter the delimiter used within fully qualified names; may be null if
     *                            {@link #DEFAULT_ID_DELIM} is to be used
     * @param leadingQuoteString  the string used for leading quotes; may be null if {@link
     *                            #DEFAULT_QUOTE} is to be used
     * @param trailingQuoteString the string used for leading quotes; may be null if {@link
     *                            #DEFAULT_QUOTE} is to be used
     */
    public IdentifierRules(
            String identifierDelimiter,
            String leadingQuoteString,
            String trailingQuoteString
    ) {
        this.leadingQuoteString = leadingQuoteString != null ? leadingQuoteString : DEFAULT_QUOTE;
        this.trailingQuoteString = trailingQuoteString != null ? trailingQuoteString : DEFAULT_QUOTE;
        this.identifierDelimiter = identifierDelimiter != null ? identifierDelimiter : DEFAULT_ID_DELIM;
    }

    public ExpressionBuilder expressionBuilder() {
        return new ExpressionBuilder(this);
    }

    /**
     * Get the delimiter that is used to delineate segments within fully-qualified identifiers.
     *
     * @return the identifier delimiter; never null
     */
    public String identifierDelimiter() {
        return identifierDelimiter;
    }


    /**
     * Get the string used as a leading quote.
     *
     * @return the leading quote string; never null
     */
    public String leadingQuoteString() {
        return leadingQuoteString;
    }

    /**
     * Get the string used as a trailing quote.
     *
     * @return the trailing quote string; never null
     */
    public String trailingQuoteString() {
        return trailingQuoteString;
    }

    /**
     * Return a new IdentifierRules that escapes quotes with the specified prefix.
     *
     * @param prefix the prefix
     * @return the new IdentifierRules, or this builder if the prefix is null or empty
     */
    public IdentifierRules escapeQuotesWith(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return this;
        }
        return new IdentifierRules(
                identifierDelimiter,
                prefix + leadingQuoteString,
                prefix + trailingQuoteString
        );
    }
}
