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
 * expression builder
 */
public class ExpressionBuilder {
    private final IdentifierRules rules;
    private final StringBuilder sb = new StringBuilder();

    public ExpressionBuilder(IdentifierRules rules) {
        this.rules = rules != null ? rules : IdentifierRules.DEFAULT;
    }

    public static ExpressionBuilder create() {
        return new ExpressionBuilder(IdentifierRules.DEFAULT);
    }

    public static ExpressionBuilder create(IdentifierRules identifierRules) {
        return new ExpressionBuilder(identifierRules);
    }

    /**
     * Get a {@link Transform} that will quote just the column names.
     *
     * @return the transform; never null
     */
    public static Transform<String> columnNames() {
        return (builder, input) -> builder.appendColumnName(input);
    }

    public static Transform<String> columnNamesWith(final String appended) {
        return (builder, input) -> {
            builder.appendColumnName(input);
            builder.append(appended);
        };
    }

    /**
     * Return a new ExpressionBuilder that escapes quotes with the specified prefix.
     * This builder remains unaffected.
     *
     * @param prefix the prefix
     * @return the new ExpressionBuilder, or this builder if the prefix is null or empty
     */
    public ExpressionBuilder escapeQuotesWith(String prefix) {
        if (prefix == null || prefix.isEmpty()) {
            return this;
        }
        return new ExpressionBuilder(this.rules.escapeQuotesWith(prefix));
    }

    /**
     * Append to this builder's expression the delimiter defined by this builder's
     * {@link IdentifierRules}.
     *
     * @return this builder to enable methods to be chained; never null
     */
    public ExpressionBuilder appendIdentifierDelimiter() {
        sb.append(rules.identifierDelimiter());
        return this;
    }

    /**
     * Always append to this builder's expression the leading quote character(s) defined by this
     * builder's {@link IdentifierRules}.
     *
     * @return this builder to enable methods to be chained; never null
     */
    public ExpressionBuilder appendLeadingQuote() {
        sb.append(rules.leadingQuoteString());
        return this;
    }

    protected ExpressionBuilder appendTrailingQuote() {
        sb.append(rules.trailingQuoteString());
        return this;
    }

    public ExpressionBuilder appendStringQuote() {
        sb.append("'");
        return this;
    }

    /**
     * Append to this builder's expression a string surrounded by single quote characters ({@code '}).
     *
     * @param name the object whose string representation is to be appended
     * @return this builder to enable methods to be chained; never null
     */
    public ExpressionBuilder appendStringQuoted(Object name) {
        appendStringQuote();
        sb.append(name);
        appendStringQuote();
        return this;
    }

    /**
     * Append to this builder's expression the identifier.
     *
     * @param name the name to be appended
     * @return this builder to enable methods to be chained; never null
     */
    public ExpressionBuilder appendIdentifier(
            String name
    ) {
        appendLeadingQuote();
        sb.append(name);
        appendTrailingQuote();
        return this;
    }

    /**
     * append table name
     *
     * @param name
     * @return
     */
    public ExpressionBuilder appendTableName(String name) {
        appendLeadingQuote();
        sb.append(name);
        appendTrailingQuote();
        return this;
    }

    /**
     * append column name
     *
     * @param name
     * @return
     */
    public ExpressionBuilder appendColumnName(String name) {
        appendLeadingQuote();
        sb.append(name);
        appendTrailingQuote();
        return this;
    }

    /**
     * Append to this builder's expression the specified identifier, surrounded by the leading and
     * trailing quotes.
     *
     * @param name the name to be appended
     * @return this builder to enable methods to be chained; never null
     */
    public ExpressionBuilder appendIdentifierQuoted(String name) {
        appendLeadingQuote();
        sb.append(name);
        appendTrailingQuote();
        return this;
    }

    /**
     * Append to this builder's expression a new line.
     *
     * @return this builder to enable methods to be chained; never null
     */
    public ExpressionBuilder appendNewLine() {
        sb.append(System.lineSeparator());
        return this;
    }

    public ExpressionBuilder append(Object obj) {
        sb.append(obj);
        return this;
    }

    public <T> ExpressionBuilder append(
            T obj,
            Transform<T> transform
    ) {
        if (transform != null) {
            transform.apply(this, obj);
        } else {
            append(obj);
        }
        return this;
    }

    public ListBuilder<Object> appendList() {
        return new BasicListBuilder<>();
    }


    public ExpressionBuilder appendMultiple(String delimiter, String expression, int times) {
        for (int i = 0; i < times; i++) {
            if (i > 0) {
                append(delimiter);
            }
            append(expression);
        }
        return this;
    }

    @Override
    public String toString() {
        return sb.toString();
    }


    /**
     * A functional interface for a transformation that an expression builder might use when
     * appending one or more other objects.
     *
     * @param <T> the type of object to transform before appending.
     */
    @FunctionalInterface
    public interface Transform<T> {
        void apply(ExpressionBuilder builder, T input);
    }


    public interface ListBuilder<T> {

        ListBuilder<T> delimitedBy(String delimiter);

        <R> ListBuilder<R> transformedBy(Transform<R> transform);

        ExpressionBuilder of(Iterable<? extends T> objects);
    }

    protected class BasicListBuilder<T> implements ListBuilder<T> {
        private final String delimiter;
        private final Transform<T> transform;
        private boolean first = true;

        BasicListBuilder() {
            this(", ", null);
        }

        BasicListBuilder(String delimiter, Transform<T> transform) {
            this.delimiter = delimiter;
            this.transform = transform != null ? transform : ExpressionBuilder::append;
        }

        @Override
        public ListBuilder<T> delimitedBy(String delimiter) {
            return new BasicListBuilder<T>(delimiter, transform);
        }

        @Override
        public <R> ListBuilder<R> transformedBy(Transform<R> transform) {
            return new BasicListBuilder<>(delimiter, transform);
        }

        @Override
        public ExpressionBuilder of(Iterable<? extends T> objects) {
            for (T obj : objects) {
                if (first) {
                    first = false;
                } else {
                    append(delimiter);
                }
                append(obj, transform);
            }
            return ExpressionBuilder.this;
        }
    }
}
