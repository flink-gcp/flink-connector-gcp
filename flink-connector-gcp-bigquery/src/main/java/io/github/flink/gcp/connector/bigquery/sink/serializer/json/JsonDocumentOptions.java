/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.bigquery.sink.serializer.json;

import org.apache.flink.annotation.Public;

import java.io.Serializable;

/**
 * Options controlling how JSON records are converted to BigQuery rows.
 *
 * <p>Unlike {@link io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions
 * ProtoSchemaOptions} and {@link
 * io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroSchemaOptions AvroSchemaOptions}
 * these carry no schema-mapping settings: a JSON document has no schema of its own, so the BigQuery
 * schema is supplied to {@link JsonDocumentSerializationSchema} directly and already says what each
 * column is — {@code JSON} columns included.
 */
@Public
public final class JsonDocumentOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final JsonDocumentOptions DEFAULTS = new JsonDocumentOptions(new Builder());

    private final boolean ignoreUnknownFields;

    private JsonDocumentOptions(Builder builder) {
        this.ignoreUnknownFields = builder.ignoreUnknownFields;
    }

    /** Returns the default options: a field the table does not have fails the record. */
    public static JsonDocumentOptions defaults() {
        return DEFAULTS;
    }

    /** Creates a new {@link Builder}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns whether fields absent from the destination schema are dropped instead of failing. */
    public boolean isIgnoreUnknownFields() {
        return ignoreUnknownFields;
    }

    /** Builder for {@link JsonDocumentOptions}. */
    @Public
    public static final class Builder {

        private boolean ignoreUnknownFields;

        Builder() {}

        /**
         * Drops fields the destination schema does not have, instead of failing the record.
         *
         * <p>By default a JSON document carrying a field the table has no column for is a row-level
         * failure, on the grounds that silently discarding data should be asked for. Ask for it
         * when the source is a document stream nobody controls — the usual case being a topic whose
         * producers add fields ahead of the table.
         *
         * @return this builder
         */
        public Builder ignoreUnknownFields() {
            this.ignoreUnknownFields = true;
            return this;
        }

        /** Builds the options. */
        public JsonDocumentOptions build() {
            return new JsonDocumentOptions(this);
        }
    }
}
