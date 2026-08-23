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

package io.github.flink.gcp.connector.bigquery.sink.serializer.proto;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.GeneratedMessage;
import io.github.flink.gcp.connector.base.options.ResourceNames;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Options controlling how protobuf descriptors are mapped to BigQuery schemas. Three mappings can
 * be adjusted:
 *
 * <ul>
 *   <li><b>JSON columns.</b> The Storage Write API carries a {@code JSON} column as a string, so
 *       this mapping is purely a schema-derivation marker: it decides whether the derived BigQuery
 *       schema says {@code JSON} rather than {@code STRUCT} or {@code STRING}. Two field types can
 *       be marked — a <b>message</b> field, which is not expanded into a {@code STRUCT} but
 *       serialized to its canonical protobuf JSON representation, and a <b>string</b> field, which
 *       is written through verbatim, its value expected to be JSON text already and not validated
 *       by the connector. Fields are selected either by their dotted path from the root message
 *       (for example {@code payload} or {@code event.details}) or by one or more boolean custom
 *       field options, each supplied as the generated extension or as its extension number.
 *       Everything configured is unioned, so a field marked by any of them is a {@code JSON}
 *       column.
 *   <li><b>Geography columns.</b> The same kind of marker, for a {@code GEOGRAPHY} column: the
 *       Storage Write API carries one as a string too, holding WKT, hex-encoded WKB or GeoJSON.
 *       Selected by dotted path or by boolean field option, exactly as a JSON column is, and
 *       unioned the same way. Only <b>string</b> fields can be marked — a message has no geography
 *       meaning — which is the one way this marker is narrower than the JSON one. Also unvalidated,
 *       so malformed geometry is a BigQuery row-level error.
 *   <li><b>Nullability.</b> Every non-repeated column is derived as {@code NULLABLE} by default.
 *       {@link Builder#deriveRequiredColumns()} reads each field's presence instead and derives
 *       {@code REQUIRED} where protobuf cannot express absence.
 * </ul>
 *
 * <p>Two reasons {@code NULLABLE} is the default. A proto3 field without presence is the spelling
 * you get by <em>not</em> thinking about nullability — {@code optional} has to be added
 * deliberately — so deriving {@code REQUIRED} from it by default would make nearly every scalar
 * column of an auto-created table {@code REQUIRED} on the strength of a syntax default. And {@code
 * REQUIRED} is the mode that cannot be walked back: BigQuery cannot add a {@code REQUIRED} column
 * to an existing table, and relaxing one is a schema update rather than an edit.
 *
 * <p>This is the normative mapping for every serializer, because every write path goes through a
 * protobuf row — the Storage Write API takes protobuf, and the Avro and JSON serializers convert
 * into one. {@link io.github.flink.gcp.connector.bigquery.sink.serializer.avro.AvroSchemaOptions
 * AvroSchemaOptions} carries the same default and the same {@code deriveRequiredColumns()} name;
 * only the signal differs — a {@code ["null", T]} union there, field presence here.
 */
@Public
public final class ProtoSchemaOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    // google.protobuf.FieldOptions declares "extensions 1000 to max", so numbers below 1000 can
    // never be one of its extensions however valid they are as ordinary field numbers.
    private static final int MIN_EXTENSION_NUMBER = 1000;
    private static final int MAX_EXTENSION_NUMBER = 536870911;
    private static final int FIRST_RESERVED_NUMBER = 19000;
    private static final int LAST_RESERVED_NUMBER = 19999;

    private static final ProtoSchemaOptions DEFAULTS = new ProtoSchemaOptions(new Builder());

    private final Set<String> jsonFieldPaths;

    /**
     * Configured field options, keyed by extension number, valued by the option's full name or
     * {@code null} when it was configured by number alone. Keyed by number because two entries for
     * one number would be contradictory — and because an unnamed entry sitting beside a named one
     * would match anything at that number, defeating the name check the named entry exists for.
     */
    private final Map<Integer, String> jsonFieldOptions;

    private final Set<String> geographyFieldPaths;

    /** As {@code jsonFieldOptions}, for {@code GEOGRAPHY} columns. */
    private final Map<Integer, String> geographyFieldOptions;

    private final boolean deriveRequiredColumns;

    private ProtoSchemaOptions(Builder builder) {
        this.jsonFieldPaths = Collections.unmodifiableSet(new HashSet<>(builder.jsonFieldPaths));
        this.jsonFieldOptions =
                Collections.unmodifiableMap(new LinkedHashMap<>(builder.jsonFieldOptions));
        this.geographyFieldPaths =
                Collections.unmodifiableSet(new HashSet<>(builder.geographyFieldPaths));
        this.geographyFieldOptions =
                Collections.unmodifiableMap(new LinkedHashMap<>(builder.geographyFieldOptions));
        this.deriveRequiredColumns = builder.deriveRequiredColumns;
    }

    /**
     * Returns the default options: no column marked, every non-repeated column {@code NULLABLE}.
     */
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

    /** Returns the dotted paths of fields mapped to BigQuery {@code GEOGRAPHY} columns. */
    public Set<String> getGeographyFieldPaths() {
        return geographyFieldPaths;
    }

    /** Returns whether column modes are derived from field presence. */
    public boolean isDeriveRequiredColumns() {
        return deriveRequiredColumns;
    }

    /**
     * Returns the configured JSON field options: extension number to the option's full name, or
     * {@code null} where it was configured by number alone. Package-private: {@link #isJsonField}
     * is the supported way to ask, and the sibling options classes expose only what the sink reads
     * back.
     */
    Map<Integer, String> getJsonFieldOptions() {
        return jsonFieldOptions;
    }

    /** As {@link #getJsonFieldOptions}, for {@code GEOGRAPHY} columns. */
    Map<Integer, String> getGeographyFieldOptions() {
        return geographyFieldOptions;
    }

    /**
     * Returns whether any of the given options is set to {@code true} on the field.
     *
     * <p>Shared by both markers so they cannot drift on what "carries this option" means. Each
     * entry may also <em>reject</em> a field whose option at that number is not a singular bool, so
     * for a field carrying both a valid option and a malformed one the registration order decides
     * between a marked column and a failure. Reaching that needs a number registered for an option
     * that is not a bool, which is already a misconfiguration.
     */
    private static boolean carriesAnyOption(
            Descriptors.FieldDescriptor field, Map<Integer, String> options) {
        for (Map.Entry<Integer, String> option : options.entrySet()) {
            if (BoolFieldOptionReader.isSetToTrue(field, option.getKey(), option.getValue())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns whether the given field is mapped to a BigQuery {@code JSON} column <em>by this
     * configuration</em>. A {@code Struct}, {@code Value} or {@code ListValue} field becomes a
     * {@code JSON} column with no configuration at all, and is not reported here; {@code
     * BigQueryProtoSerializationSchema#getTableSchema} is the derived truth.
     *
     * <p>The configured path and every configured field option are consulted, and a field selected
     * by any of them is a JSON column (see {@link #carriesAnyOption} for what an option match
     * involves).
     *
     * @param field the field descriptor
     * @param path the dotted path of the field from the root message
     * @return whether the field is written as JSON
     */
    public boolean isJsonField(Descriptors.FieldDescriptor field, String path) {
        return jsonFieldPaths.contains(path) || carriesAnyOption(field, jsonFieldOptions);
    }

    /**
     * Returns whether the given field is mapped to a BigQuery {@code GEOGRAPHY} column. Selected by
     * dotted path or by field option exactly as {@link #isJsonField} is, and unioned the same way.
     *
     * @param field the field descriptor
     * @param path the dotted path of the field from the root message
     * @return whether the field is written as geography
     */
    public boolean isGeographyField(Descriptors.FieldDescriptor field, String path) {
        return geographyFieldPaths.contains(path) || carriesAnyOption(field, geographyFieldOptions);
    }

    /**
     * Returns the BigQuery type the given field is <em>configured</em> as being marked with, or
     * {@code null} where it is marked with none, and rejects a field claimed by both markers.
     *
     * <p>Deliberately <b>not</b> the whole answer, and named so: the automatic {@code JSON} of
     * {@code Struct}, {@code Value} and {@code ListValue} is not configuration and is folded on top
     * by {@link ProtoToTableSchemaConverter#markedType}, which <em>is</em> the single decision
     * point both schema derivation and row conversion consult. Calling this one where that one
     * belongs would silently miss those columns and bring back the disagreement between the derived
     * schema and the row plan that having one decision point exists to prevent.
     *
     * @param field the field descriptor
     * @param path the dotted path of the field from the root message
     * @return {@code JSON}, {@code GEOGRAPHY}, or {@code null}
     */
    TableFieldSchema.Type configuredMarkedType(Descriptors.FieldDescriptor field, String path) {
        boolean json = isJsonField(field, path);
        boolean geography = isGeographyField(field, path);
        // A column has one type, so a field claimed by both markers is a configuration error rather
        // than a precedence question. Checked here because this is the one place both are visible:
        // an option cannot be intersected with a path without a descriptor, and neither can two
        // *different* option numbers that happen to meet on one field. One number registered as
        // both markers needs no descriptor and is rejected earlier, in build().
        Preconditions.checkArgument(
                !(json && geography),
                "Field %s is marked as both a JSON and a GEOGRAPHY column",
                path);
        if (json) {
            return TableFieldSchema.Type.JSON;
        }
        return geography ? TableFieldSchema.Type.GEOGRAPHY : null;
    }

    /** Builder for {@link ProtoSchemaOptions}. */
    @Public
    public static final class Builder {

        private final Set<String> jsonFieldPaths = new HashSet<>();
        private final Map<Integer, String> jsonFieldOptions = new LinkedHashMap<>();
        private final Set<String> geographyFieldPaths = new HashSet<>();
        private final Map<Integer, String> geographyFieldOptions = new LinkedHashMap<>();
        private boolean deriveRequiredColumns;

        Builder() {}

        /**
         * Derives each column's mode from its field's presence, instead of deriving every
         * non-repeated column as {@code NULLABLE}. Nested message fields are covered too, and so
         * are map entry columns — a proto3 entry's {@code key} and {@code value} have implicit
         * presence, so both become {@code REQUIRED}. Repeated fields are unaffected, since a
         * BigQuery {@code REPEATED} column cannot be {@code NULLABLE}.
         *
         * <p>The resulting map:
         *
         * <table>
         *   <caption>Presence to BigQuery mode</caption>
         *   <tr><th>Field</th><th>Mode</th></tr>
         *   <tr><td>{@code repeated}, including maps</td><td>{@code REPEATED}</td></tr>
         *   <tr><td>plain proto3 singular scalar or enum</td><td>{@code REQUIRED}</td></tr>
         *   <tr><td>proto3 {@code optional}</td><td>{@code NULLABLE}</td></tr>
         *   <tr><td>{@code oneof} member</td><td>{@code NULLABLE}</td></tr>
         *   <tr><td>singular message field</td><td>{@code NULLABLE}</td></tr>
         *   <tr><td>proto2 {@code required}</td><td>{@code REQUIRED}</td></tr>
         *   <tr><td>proto2 {@code optional}</td><td>{@code NULLABLE}</td></tr>
         *   <tr><td>singular {@code JSON} or {@code GEOGRAPHY} column</td>
         *       <td>{@code NULLABLE}, always</td></tr>
         *   <tr><td>singular well-known type, wrappers included</td>
         *       <td>{@code NULLABLE} — a message field, so it has presence</td></tr>
         * </table>
         *
         * <p>A plain proto3 scalar cannot say "unset" — an unset value is indistinguishable from
         * the type default — so {@code REQUIRED} is the faithful column mode for it, and the one
         * the value path already satisfies: such a field is always written, as {@code 0}, {@code
         * ""} or the first enum value. Proto2 {@code required} is called out separately because it
         * <em>has</em> presence and is mandatory all the same, so presence alone would map the one
         * unambiguous case to {@code NULLABLE}.
         *
         * <p>This changes only the derived schema — the one used for table auto-creation, for the
         * write stream and for load jobs. Values are converted identically either way, and toggling
         * it changes protobuf labels rather than the encoding of any value, so rows already
         * serialized stay valid.
         *
         * <p>Two consequences worth weighing before enabling it. A record that leaves a {@code
         * REQUIRED}-derived field unset — reachable only through a proto2 {@code required} field on
         * a partially built message — becomes a row-level failure routed to the configured {@code
         * FailureHandler}. And BigQuery cannot add a {@code REQUIRED} column to an existing table,
         * so a column derived this way is only ever created with the table.
         *
         * @return this builder
         */
        public Builder deriveRequiredColumns() {
            this.deriveRequiredColumns = true;
            return this;
        }

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
         * <!-- javadoc-example file="JavadocBigQueryExamples.java" tag="json-option" -->
         *
         * <pre>{@code
         * ProtoSchemaOptions.builder().jsonFieldOption(MyAnnotations.json).build();
         * }</pre>
         *
         * <p>The extension itself is not retained: it holds a protobuf descriptor and is not
         * Java-serializable, while these options travel in the job graph. Only its number and name
         * are kept.
         *
         * <p>Additive, like {@link #jsonFieldPath}: several annotation vocabularies can be marked.
         * Registering the same number twice keeps the entry that carries a name, since an unnamed
         * one would match anything at that number and defeat the check the named one exists for.
         * Where two extensions claim one number the last call wins — only one entry per number is
         * kept, and neither choice is self-evidently right.
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
            checkExtensionNumber(descriptor.getNumber());
            // A name always wins over a bare number for the same option.
            this.jsonFieldOptions.put(descriptor.getNumber(), descriptor.getFullName());
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
            Preconditions.checkNotNull(path, "path must not be null");
            this.jsonFieldPaths.add(ResourceNames.checkNotBlank(path, "path"));
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
         * Maps the string field at the given dotted path to a BigQuery {@code GEOGRAPHY} column.
         * Paths that match no field, that match a field which is not a string, or that are also
         * marked as a {@code JSON} column are rejected when the schema is derived.
         *
         * <p>Only a string field can be marked, where {@link #jsonFieldPath} also accepts a
         * message: a message has no geography meaning, there being no protobuf type BigQuery would
         * recognise as one. The value must be one of the text forms BigQuery accepts for a
         * geography — WKT, hex-encoded WKB or GeoJSON — and is passed through verbatim without
         * being validated by the connector, so malformed geometry is a BigQuery row-level error
         * routed to the configured {@code FailureHandler}.
         *
         * <p>Where the mapping is a property of the schema rather than of the pipeline, {@link
         * #geographyFieldOption} marks the same columns by annotation instead; the two are unioned.
         *
         * @param path dotted field path from the root message, for example {@code site.boundary}
         * @return this builder
         */
        public Builder geographyFieldPath(String path) {
            Preconditions.checkNotNull(path, "path must not be null");
            this.geographyFieldPaths.add(ResourceNames.checkNotBlank(path, "path"));
            return this;
        }

        /**
         * Maps all string fields at the given dotted paths to BigQuery {@code GEOGRAPHY} columns.
         *
         * @param paths dotted field paths from the root message
         * @return this builder
         */
        public Builder geographyFieldPaths(Collection<String> paths) {
            Preconditions.checkNotNull(paths, "paths must not be null")
                    .forEach(this::geographyFieldPath);
            return this;
        }

        /**
         * Maps every string field carrying the given boolean field option, set to {@code true}, to
         * a BigQuery {@code GEOGRAPHY} column — wherever it appears in the message tree, at any
         * nesting depth.
         *
         * <p>The geography counterpart of {@link #jsonFieldOption}, with the same mechanics,
         * guarantees and caveats: prefer it over {@link #geographyFieldOptionNumber} whenever the
         * generated extension class is on the classpath, since the compiler enforces that the
         * option really is a {@code bool} and the option's full name is captured so an unrelated
         * option sharing the number is ignored wherever the declaration can be resolved. The
         * extension itself is not retained (it holds a descriptor and is not Java-serializable,
         * while these options travel in the job graph) — only its number and name. Additive, and
         * where two extensions claim one number the last call wins.
         * <!-- javadoc-example file="JavadocBigQueryExamples.java" tag="geography-option" -->
         *
         * <pre>{@code
         * ProtoSchemaOptions.builder().geographyFieldOption(MyAnnotations.geography).build();
         * }</pre>
         *
         * <p>The option is declared exactly as a JSON one is — a {@code bool} extension of {@code
         * google.protobuf.FieldOptions}. What differs is the field it may mark: a string and
         * nothing else, protobuf having no geometry type for a message to be. A non-string field
         * carrying the option is <b>rejected</b> when the schema is derived, not skipped — which
         * matters more here than for a path, since an annotation applied across a corpus selects
         * fields you did not enumerate, and one landing on a message field fails the job.
         *
         * @param extension the generated extension for a {@code bool} option on {@code
         *     google.protobuf.FieldOptions}
         * @return this builder
         */
        public Builder geographyFieldOption(
                GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, Boolean>
                        extension) {
            Descriptors.FieldDescriptor descriptor =
                    Preconditions.checkNotNull(extension, "extension must not be null")
                            .getDescriptor();
            checkExtensionNumber(descriptor.getNumber());
            // A name always wins over a bare number for the same option.
            this.geographyFieldOptions.put(descriptor.getNumber(), descriptor.getFullName());
            return this;
        }

        /**
         * Maps every string field carrying the given boolean {@code google.protobuf.FieldOptions}
         * extension, set to {@code true}, to a BigQuery {@code GEOGRAPHY} column — wherever it
         * appears in the message tree, at any nesting depth.
         *
         * <p>The geography counterpart of {@link #jsonFieldOptionNumber}, with the same mechanics
         * and caveats — including that it is additive, and that a number {@link
         * #geographyFieldOption} already supplied a name for keeps the name. Two are worth
         * restating here. The number alone is not an identity, protobuf's private extension range
         * having no registry. And unlike {@link #geographyFieldPath}, a number matching no field is
         * <em>not</em> an error — one configuration is meant to serve every message type a job
         * writes, and a message legitimately need not have geography columns — so a mistyped number
         * yields {@code STRING} columns instead of failing. Check the outcome with {@code
         * BigQueryProtoSerializationSchema#getTableSchema}.
         *
         * @param extensionNumber the extension number of the option within {@code
         *     google.protobuf.FieldOptions}
         * @return this builder
         */
        public Builder geographyFieldOptionNumber(int extensionNumber) {
            checkExtensionNumber(extensionNumber);
            // containsKey, not putIfAbsent: a bare number is stored as a null value, which both
            // putIfAbsent and computeIfAbsent key off. See jsonFieldOptionNumber.
            if (!this.geographyFieldOptions.containsKey(extensionNumber)) {
                this.geographyFieldOptions.put(extensionNumber, null);
            }
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
         * BigQueryProtoSerializationSchema#getTableSchema}.
         *
         * <p>Additive, like {@link #jsonFieldPath}. Registering a number that {@link
         * #jsonFieldOption} already supplied a name for keeps the name: an unnamed entry beside a
         * named one would match anything at that number and defeat the check.
         *
         * @param extensionNumber the extension number of the option within {@code
         *     google.protobuf.FieldOptions}
         * @return this builder
         */
        public Builder jsonFieldOptionNumber(int extensionNumber) {
            checkExtensionNumber(extensionNumber);
            // A name always wins, so never displace an existing entry. Deliberately containsKey and
            // not putIfAbsent or computeIfAbsent: both key off a null *value*, and a bare number is
            // stored as exactly that — computeIfAbsent would not even add the key.
            if (!this.jsonFieldOptions.containsKey(extensionNumber)) {
                this.jsonFieldOptions.put(extensionNumber, null);
            }
            return this;
        }

        private static void checkExtensionNumber(int extensionNumber) {
            Preconditions.checkArgument(
                    extensionNumber >= MIN_EXTENSION_NUMBER
                            && extensionNumber <= MAX_EXTENSION_NUMBER
                            && (extensionNumber < FIRST_RESERVED_NUMBER
                                    || extensionNumber > LAST_RESERVED_NUMBER),
                    "A field option number must be a google.protobuf.FieldOptions extension"
                            + " number in [%s, %s] and outside protobuf's reserved range [%s, %s],"
                            + " but was %s",
                    MIN_EXTENSION_NUMBER,
                    MAX_EXTENSION_NUMBER,
                    FIRST_RESERVED_NUMBER,
                    LAST_RESERVED_NUMBER,
                    extensionNumber);
        }

        /**
         * Builds the options, rejecting one extension number registered as <em>both</em> a JSON and
         * a geography option.
         *
         * <p>That is the one marker contradiction visible without a descriptor: it says every field
         * carrying that annotation is both kinds of column at once, so it is broken for every
         * message rather than for some. The collision that needs a descriptor — two
         * <em>different</em> numbers, or an option against a path, meeting on one field — is
         * rejected at schema derivation instead, where both are visible. Two checks, because they
         * are two rules.
         */
        public ProtoSchemaOptions build() {
            Set<Integer> both = new HashSet<>(jsonFieldOptions.keySet());
            both.retainAll(geographyFieldOptions.keySet());
            Preconditions.checkArgument(
                    both.isEmpty(),
                    "Field option numbers registered as both a JSON and a GEOGRAPHY option: %s",
                    both);
            return new ProtoSchemaOptions(this);
        }
    }
}
