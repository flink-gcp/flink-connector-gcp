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

package io.github.flink.gcp.connector.bigquery.sink.serializer.avro;

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
 *       the protobuf path (see {@link
 *       io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions
 *       ProtoSchemaOptions}).
 *   <li><b>Nullability.</b> Every non-repeated column is derived as {@code NULLABLE} by default.
 *       {@link Builder#deriveRequiredColumns()} reads the Avro schema instead and derives {@code
 *       REQUIRED} for any field that is not a {@code ["null", T]} union.
 * </ul>
 *
 * <p>Two reasons {@code NULLABLE} is the default. {@code REQUIRED} is the mode BigQuery cannot walk
 * back — it cannot be added to an existing table, so such a column only ever appears at creation
 * time and relaxing one afterwards is a schema update rather than an edit. And the protobuf mapping
 * is the normative one for every serializer, because every write path ends in a protobuf row: the
 * Storage Write API takes protobuf, and this serializer converts into one. An Avro {@code ["null",
 * T]} union is admittedly the schema author's own statement, which makes {@code REQUIRED} the more
 * faithful reading of an Avro schema taken alone — that is why this side used to default to it —
 * but faithfulness to one front end does not outweigh agreeing with the wire form every path
 * shares.
 */
@PublicEvolving
public final class AvroSchemaOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final AvroSchemaOptions DEFAULTS = new AvroSchemaOptions(new Builder());

    private final Set<String> jsonFieldPaths;
    private final boolean deriveRequiredColumns;

    private AvroSchemaOptions(Builder builder) {
        this.jsonFieldPaths = Collections.unmodifiableSet(new HashSet<>(builder.jsonFieldPaths));
        this.deriveRequiredColumns = builder.deriveRequiredColumns;
    }

    /**
     * Returns the default options: no JSON field mapping, every non-repeated column {@code
     * NULLABLE}.
     */
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

    /** Returns whether column modes are derived from the Avro schema. */
    public boolean isDeriveRequiredColumns() {
        return deriveRequiredColumns;
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
        private boolean deriveRequiredColumns;

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
         * Derives each column's mode from the Avro schema, instead of deriving every non-repeated
         * column as {@code NULLABLE}: a field that is not a {@code ["null", T]} union becomes
         * {@code REQUIRED}. Nested record fields and map entry columns are covered too; {@code
         * REPEATED} fields are unaffected, since a BigQuery {@code REPEATED} column cannot be
         * {@code NULLABLE}.
         *
         * <p>Named as on the protobuf side ({@link
         * io.github.flink.gcp.connector.bigquery.sink.serializer.proto.ProtoSchemaOptions.Builder#deriveRequiredColumns()
         * ProtoSchemaOptions.Builder.deriveRequiredColumns()}) because the two mean the same thing;
         * only the signal differs — a {@code ["null", T]} union here, field presence there.
         *
         * <p>This changes only the derived schema — the one used for table auto-creation, for the
         * write stream and for load jobs. Records that carry the value convert identically either
         * way.
         *
         * <p>Two consequences to weigh. A record that <em>omits</em> a field the Avro schema
         * declares mandatory becomes a row-level failure routed to the configured {@code
         * FailedRowHandler}, where by default the column is simply left unset. And BigQuery cannot
         * add a {@code REQUIRED} column to an existing table, so a column derived this way is only
         * ever created together with the table.
         *
         * @return this builder
         */
        public Builder deriveRequiredColumns() {
            this.deriveRequiredColumns = true;
            return this;
        }

        /** Builds the options. */
        public AvroSchemaOptions build() {
            return new AvroSchemaOptions(this);
        }
    }
}
