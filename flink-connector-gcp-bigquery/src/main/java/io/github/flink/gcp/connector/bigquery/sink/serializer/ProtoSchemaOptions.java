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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Options controlling how protobuf descriptors are mapped to BigQuery schemas.
 *
 * <p>Currently supports marking fields as BigQuery {@code JSON} columns. The Storage Write API
 * carries a {@code JSON} column as a string, so this mapping is purely a schema-derivation marker:
 * it decides whether the derived BigQuery schema says {@code JSON} rather than {@code STRUCT} or
 * {@code STRING}. Two field types can be marked:
 *
 * <ul>
 *   <li>a <b>message</b> field is not expanded into a {@code STRUCT}; it is serialized to its
 *       canonical protobuf JSON representation
 *   <li>a <b>string</b> field is written through verbatim — its value is expected to be JSON text
 *       already, and is not validated by the connector
 * </ul>
 *
 * <p>Fields are selected either by their dotted path from the root message (for example {@code
 * payload} or {@code event.details}) or by a boolean custom field option, supplied either as the
 * generated extension or as its extension number. The two mechanisms are unioned, so a field marked
 * either way is a {@code JSON} column.
 */
@PublicEvolving
public final class ProtoSchemaOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Sentinel for {@link #jsonFieldOptionNumber}: no field option is configured. */
    private static final int NO_FIELD_OPTION = 0;

    // google.protobuf.FieldOptions declares "extensions 1000 to max", so numbers below 1000 can
    // never be one of its extensions however valid they are as ordinary field numbers.
    private static final int MIN_EXTENSION_NUMBER = 1000;
    private static final int MAX_EXTENSION_NUMBER = 536870911;
    private static final int FIRST_RESERVED_NUMBER = 19000;
    private static final int LAST_RESERVED_NUMBER = 19999;

    private static final ProtoSchemaOptions DEFAULTS = new ProtoSchemaOptions(new Builder());

    private final Set<String> jsonFieldPaths;
    private final int jsonFieldOptionNumber;
    private final String jsonFieldOptionName;

    private ProtoSchemaOptions(Builder builder) {
        this.jsonFieldPaths = Collections.unmodifiableSet(new HashSet<>(builder.jsonFieldPaths));
        this.jsonFieldOptionNumber = builder.jsonFieldOptionNumber;
        this.jsonFieldOptionName = builder.jsonFieldOptionName;
    }

    /** Returns the default options: no JSON field mapping. */
    public static ProtoSchemaOptions defaults() {
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

    /**
     * Returns the {@code google.protobuf.FieldOptions} extension number marking {@code JSON}
     * columns, or {@code 0} if no field option is configured. Package-private: {@link #isJsonField}
     * is the supported way to ask, and the sibling options classes expose only what the sink itself
     * reads back.
     */
    int getJsonFieldOptionNumber() {
        return jsonFieldOptionNumber;
    }

    /**
     * Returns the full name of the field option marking {@code JSON} columns, or {@code null} when
     * the option was configured by number alone.
     */
    String getJsonFieldOptionName() {
        return jsonFieldOptionName;
    }

    /**
     * Returns whether the given field is mapped to a BigQuery {@code JSON} column. This is the
     * single decision point consulted by both schema derivation and row conversion, so the two can
     * never disagree on which columns are JSON.
     *
     * @param field the field descriptor
     * @param path the dotted path of the field from the root message
     * @return whether the field is written as JSON
     */
    public boolean isJsonField(Descriptors.FieldDescriptor field, String path) {
        return jsonFieldPaths.contains(path)
                || (jsonFieldOptionNumber != NO_FIELD_OPTION
                        && BoolFieldOptionReader.isSetToTrue(
                                field, jsonFieldOptionNumber, jsonFieldOptionName));
    }

    /** Builder for {@link ProtoSchemaOptions}. */
    @PublicEvolving
    public static final class Builder {

        private final Set<String> jsonFieldPaths = new HashSet<>();
        private int jsonFieldOptionNumber = NO_FIELD_OPTION;
        private String jsonFieldOptionName;

        Builder() {}

        /**
         * Maps every message or string field carrying the given boolean field option, set to {@code
         * true}, to a BigQuery {@code JSON} column — wherever it appears in the message tree, at
         * any nesting depth.
         *
         * <p>Prefer this over {@link #jsonFieldOptionNumber} whenever the generated extension class
         * is on the classpath. It is the same mechanism, but the compiler enforces that the option
         * really is a {@code bool}, and the option's full name is captured so that an unrelated
         * option sharing the extension number — which protobuf's private range makes possible,
         * since it has no registry — is ignored wherever its declaration can be resolved. That is
         * everywhere the annotations proto is either on the classpath or among the descriptor's
         * transitive dependencies; a descriptor that arrives with neither leaves the number as the
         * only available identity.
         *
         * <pre>{@code
         * ProtoSchemaOptions.builder().jsonFieldOption(MyAnnotations.json).build();
         * }</pre>
         *
         * <p>The extension itself is not retained: it holds a protobuf descriptor and is not
         * Java-serializable, while these options travel in the job graph. Only its number and name
         * are kept.
         *
         * @param extension the generated extension for a {@code bool} option on {@code
         *     google.protobuf.FieldOptions}
         * @return this builder
         */
        public Builder jsonFieldOption(
                GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, Boolean>
                        extension) {
            Descriptors.FieldDescriptor descriptor =
                    Preconditions.checkNotNull(extension, "extension must not be null")
                            .getDescriptor();
            jsonFieldOptionNumber(descriptor.getNumber());
            this.jsonFieldOptionName = descriptor.getFullName();
            return this;
        }

        /**
         * Maps the message or string field at the given dotted path to a BigQuery {@code JSON}
         * column. Paths that match no field are rejected when the schema is derived.
         *
         * @param path dotted field path from the root message, for example {@code event.details}
         * @return this builder
         */
        public Builder jsonFieldPath(String path) {
            this.jsonFieldPaths.add(Preconditions.checkNotNull(path, "path must not be null"));
            return this;
        }

        /**
         * Maps all message or string fields at the given dotted paths to BigQuery {@code JSON}
         * columns.
         *
         * @param paths dotted field paths from the root message
         * @return this builder
         */
        public Builder jsonFieldPaths(Collection<String> paths) {
            Preconditions.checkNotNull(paths, "paths must not be null")
                    .forEach(this::jsonFieldPath);
            return this;
        }

        /**
         * Maps every message or string field carrying the given boolean {@code
         * google.protobuf.FieldOptions} extension, set to {@code true}, to a BigQuery {@code JSON}
         * column — wherever it appears in the message tree, at any nesting depth.
         *
         * <p>Use this when the generated extension class is <em>not</em> on the classpath; prefer
         * {@link #jsonFieldOption} when it is. Only the extension number is needed: the option is
         * found whether the descriptor knows it as a registered extension or carries it as an
         * unknown field. An existing private extension number can therefore be adopted as-is, with
         * no change to the protobuf sources.
         *
         * <p>The number alone is not an identity — protobuf's private extension range has no
         * registry, so an unrelated annotation can occupy it. The declaration is checked for a
         * {@code bool} type wherever it can be resolved, but two {@code bool} options at the same
         * number are indistinguishable without a name.
         *
         * <p>Unlike {@link #jsonFieldPath}, a number that matches no field is <em>not</em> an error
         * — one configuration is meant to serve every message type a job writes, and a message
         * legitimately need not have JSON columns — so a mistyped number yields {@code STRING} or
         * {@code STRUCT} columns instead of failing. Check the outcome with {@code
         * BigQueryProtoSerializer#getTableSchema}.
         *
         * <p>A job annotates its fields with one vocabulary, so this is a single number: calling
         * this more than once replaces the previous one rather than adding to it.
         *
         * @param extensionNumber the extension number of the option within {@code
         *     google.protobuf.FieldOptions}
         * @return this builder
         */
        public Builder jsonFieldOptionNumber(int extensionNumber) {
            Preconditions.checkArgument(
                    extensionNumber >= MIN_EXTENSION_NUMBER
                            && extensionNumber <= MAX_EXTENSION_NUMBER
                            && (extensionNumber < FIRST_RESERVED_NUMBER
                                    || extensionNumber > LAST_RESERVED_NUMBER),
                    "jsonFieldOptionNumber must be a google.protobuf.FieldOptions extension number"
                            + " in [%s, %s] and outside protobuf's reserved range [%s, %s], but was"
                            + " %s",
                    MIN_EXTENSION_NUMBER,
                    MAX_EXTENSION_NUMBER,
                    FIRST_RESERVED_NUMBER,
                    LAST_RESERVED_NUMBER,
                    extensionNumber);
            // Only after the check, so a rejected number cannot leave the previous number paired
            // with no name — that would silently downgrade the collision guard for a caller that
            // catches the exception. jsonFieldOption re-assigns the name after delegating here.
            this.jsonFieldOptionName = null;
            this.jsonFieldOptionNumber = extensionNumber;
            return this;
        }

        /** Builds the options. */
        public ProtoSchemaOptions build() {
            return new ProtoSchemaOptions(this);
        }
    }
}
