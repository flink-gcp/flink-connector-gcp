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
 * Options controlling how Avro schemas are mapped to BigQuery schemas.
 *
 * <p>Two mappings can be adjusted:
 *
 * <ul>
 *   <li><b>JSON columns.</b> An Avro {@code string} field selected by its dotted path becomes a
 *       BigQuery {@code JSON} column instead of {@code STRING}. The Storage Write API carries a
 *       {@code JSON} column as a string, so this is purely a schema-derivation marker — the value
 *       is written through verbatim and is <em>not</em> validated by the connector, exactly as on
 *       the protobuf path (see {@link ProtoSchemaOptions}).
 *   <li><b>Nullability.</b> By default an Avro field that is not a {@code ["null", T]} union maps
 *       to a {@code REQUIRED} column. {@link Builder#allFieldsNullable()} relaxes every column to
 *       {@code NULLABLE} instead, which is what a pipeline wants when the destination table should
 *       tolerate fields the source schema happens to declare mandatory today.
 * </ul>
 */
@PublicEvolving
public final class AvroSchemaOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final AvroSchemaOptions DEFAULTS = new AvroSchemaOptions(new Builder());

    private final Set<String> jsonFieldPaths;
    private final boolean allFieldsNullable;

    private AvroSchemaOptions(Builder builder) {
        this.jsonFieldPaths = Collections.unmodifiableSet(new HashSet<>(builder.jsonFieldPaths));
        this.allFieldsNullable = builder.allFieldsNullable;
    }

    /** Returns the default options: no JSON field mapping, nullability taken from the schema. */
    public static AvroSchemaOptions defaults() {
        return DEFAULTS;
    }

    /** Creates a new {@link Builder}. */
    public static Builder builder() {
        return new Builder();
    }

    /** Returns the dotted paths of fields mapped to BigQuery {@code JSON} columns. */
    public Set<String> getJsonFieldPaths() {
        return jsonFieldPaths;
    }

    /** Returns whether every derived column is forced to {@code NULLABLE}. */
    public boolean isAllFieldsNullable() {
        return allFieldsNullable;
    }

    /**
     * Returns whether the field at the given dotted path is mapped to a BigQuery {@code JSON}
     * column. This is the single decision point consulted by both schema derivation and row
     * conversion, so the two can never disagree on which columns are JSON.
     *
     * @param path the dotted path of the field from the root record
     * @return whether the field is written as JSON
     */
    public boolean isJsonField(String path) {
        return jsonFieldPaths.contains(path);
    }

    /** Builder for {@link AvroSchemaOptions}. */
    @PublicEvolving
    public static final class Builder {

        private final Set<String> jsonFieldPaths = new HashSet<>();
        private boolean allFieldsNullable;

        Builder() {}

        /**
         * Maps the {@code string} field at the given dotted path to a BigQuery {@code JSON} column.
         * Paths that match no field, or that match a field which is not a {@code string}, are
         * rejected when the schema is derived.
         *
         * @param path dotted field path from the root record, for example {@code event.details}
         * @return this builder
         */
        public Builder jsonFieldPath(String path) {
            this.jsonFieldPaths.add(Preconditions.checkNotNull(path, "path must not be null"));
            return this;
        }

        /**
         * Maps all {@code string} fields at the given dotted paths to BigQuery {@code JSON}
         * columns.
         *
         * @param paths dotted field paths from the root record
         * @return this builder
         */
        public Builder jsonFieldPaths(Collection<String> paths) {
            Preconditions.checkNotNull(paths, "paths must not be null")
                    .forEach(this::jsonFieldPath);
            return this;
        }

        /**
         * Derives every column as {@code NULLABLE}, overriding the {@code REQUIRED} mode an Avro
         * field that is not a {@code ["null", T]} union would otherwise produce. Nested record
         * fields are relaxed too; {@code REPEATED} fields are not affected, since a BigQuery {@code
         * REPEATED} field cannot be {@code NULLABLE}.
         *
         * <p>This changes only the derived schema — the one used for table auto-creation, for the
         * write stream and for load jobs. Values are converted identically either way: a field the
         * Avro schema declares mandatory still always carries a value.
         *
         * @return this builder
         */
        public Builder allFieldsNullable() {
            this.allFieldsNullable = true;
            return this;
        }

        /** Builds the options. */
        public AvroSchemaOptions build() {
            return new AvroSchemaOptions(this);
        }
    }
}
