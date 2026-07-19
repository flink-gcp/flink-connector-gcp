/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Options controlling how protobuf descriptors are mapped to BigQuery schemas.
 *
 * <p>Currently supports marking message fields as BigQuery {@code JSON} columns: instead of
 * expanding the message into a {@code STRUCT}, the sub-message is serialized to its canonical
 * protobuf JSON representation and written into a {@code JSON} column. Fields are addressed by
 * their dotted path from the root message (for example {@code payload} or {@code event.details}).
 */
@PublicEvolving
public final class ProtoSchemaOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ProtoSchemaOptions DEFAULTS = new ProtoSchemaOptions(new Builder());

    private final Set<String> jsonFieldPaths;

    private ProtoSchemaOptions(Builder builder) {
        this.jsonFieldPaths = Collections.unmodifiableSet(new HashSet<>(builder.jsonFieldPaths));
    }

    /** Returns the default options: no JSON field mapping. */
    public static ProtoSchemaOptions defaults() {
        return DEFAULTS;
    }

    /** Creates a new {@link Builder}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the dotted paths of message fields mapped to BigQuery {@code JSON} columns. */
    public Set<String> getJsonFieldPaths() {
        return jsonFieldPaths;
    }

    /**
     * Returns whether the given field is mapped to a BigQuery {@code JSON} column. This is the
     * single decision point consulted by both schema derivation and row conversion (a
     * proto-field-option based selection will extend it here).
     *
     * @param field the field descriptor
     * @param path the dotted path of the field from the root message
     * @return whether the field is written as JSON
     */
    public boolean isJsonField(
            @SuppressWarnings("unused") com.google.protobuf.Descriptors.FieldDescriptor field,
            String path) {
        return jsonFieldPaths.contains(path);
    }

    /** Builder for {@link ProtoSchemaOptions}. */
    @PublicEvolving
    public static final class Builder {

        private final Set<String> jsonFieldPaths = new HashSet<>();

        Builder() {}

        /**
         * Maps the message field at the given dotted path to a BigQuery {@code JSON} column.
         *
         * @param path dotted field path from the root message, for example {@code event.details}
         * @return this builder
         */
        public Builder jsonFieldPath(String path) {
            this.jsonFieldPaths.add(Preconditions.checkNotNull(path, "path must not be null"));
            return this;
        }

        /**
         * Maps all message fields at the given dotted paths to BigQuery {@code JSON} columns.
         *
         * @param paths dotted field paths from the root message
         * @return this builder
         */
        public Builder jsonFieldPaths(Collection<String> paths) {
            Preconditions.checkNotNull(paths, "paths must not be null")
                    .forEach(this::jsonFieldPath);
            return this;
        }

        /** Builds the options. */
        public ProtoSchemaOptions build() {
            return new ProtoSchemaOptions(this);
        }
    }
}
