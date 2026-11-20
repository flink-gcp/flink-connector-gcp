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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import com.google.cloud.bigquery.storage.v1.BQTableSchemaToProtoDescriptor;
import com.google.cloud.bigquery.storage.v1.BigQuerySchemaUtil;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDescriptors}. */
class RowDescriptorsTest {

    private static TableFieldSchema field(String name, TableFieldSchema.Type type) {
        return TableFieldSchema.newBuilder()
                .setName(name)
                .setType(type)
                .setMode(TableFieldSchema.Mode.NULLABLE)
                .build();
    }

    @Test
    void derivesARowDescriptorCarryingTheSchemasColumns() {
        Descriptors.Descriptor descriptor =
                RowDescriptors.derive(
                        TableSchema.newBuilder()
                                .addFields(field("name", TableFieldSchema.Type.STRING))
                                .addFields(field("n", TableFieldSchema.Type.INT64))
                                .build(),
                        "the supplied schema");

        assertThat(descriptor.getFields())
                .extracting(Descriptors.FieldDescriptor::getName)
                .containsExactly("name", "n");
    }

    /** A schema whose column names hold an upper-case {@code I}, at the top level and nested. */
    private static TableSchema mixedCase() {
        return TableSchema.newBuilder()
                .addFields(field("userId", TableFieldSchema.Type.INT64))
                .addFields(
                        field("Items", TableFieldSchema.Type.STRUCT).toBuilder()
                                .addFields(field("innerValue", TableFieldSchema.Type.STRING))
                                .addFields(field("Info", TableFieldSchema.Type.INT64))
                                .build())
                .build();
    }

    /**
     * The library lower-cases with the <em>default</em> locale, so under {@code tr_TR} it names the
     * field for {@code userId} with a placeholder whose column name is {@code userıd}, which
     * BigQuery rejects. The names reach it lower-cased under {@link Locale#ROOT}, which no locale
     * changes again (#2188).
     */
    @Test
    void lowerCasesColumnNamesTheSameWayUnderEveryDefaultLocale() throws Exception {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            // The premise, so that this test cannot pass without exercising the divergence.
            Descriptors.Descriptor libraryAlone =
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                            mixedCase());
            assertThat(BigQuerySchemaUtil.getFieldName(libraryAlone.getFields().get(0)))
                    .isEqualTo("user\u0131d");

            Descriptors.Descriptor descriptor = RowDescriptors.derive(mixedCase(), "the schema");

            assertThat(descriptor.getFields())
                    .extracting(Descriptors.FieldDescriptor::getName)
                    .containsExactly("userid", "items");
            Descriptors.Descriptor item = descriptor.getFields().get(1).getMessageType();
            assertThat(item.getFields())
                    .extracting(Descriptors.FieldDescriptor::getName)
                    .containsExactly("innervalue", "info");
            // Plain protobuf names, so no placeholder carries a column name of its own.
            assertThat(descriptor.getFields())
                    .allSatisfy(
                            field ->
                                    assertThat(BigQuerySchemaUtil.getFieldName(field))
                                            .isEqualTo(field.getName()));
            assertThat(BigQuerySchemaUtil.getFieldName(item.getFields().get(1))).isEqualTo("info");
        } finally {
            Locale.setDefault(previous);
        }
    }

    /** A name that lower-cases differently only below the top level is found there too. */
    @Test
    void lowerCasesWhenOnlyANestedNameDiffers() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            TableSchema nestedOnly =
                    TableSchema.newBuilder()
                            .addFields(
                                    field("Name", TableFieldSchema.Type.STRUCT).toBuilder()
                                            .addFields(field("Info", TableFieldSchema.Type.INT64))
                                            .build())
                            .build();

            Descriptors.FieldDescriptor info =
                    RowDescriptors.derive(nestedOnly, "the schema")
                            .getFields()
                            .get(0)
                            .getMessageType()
                            .getFields()
                            .get(0);

            assertThat(info.getName()).isEqualTo("info");
            assertThat(BigQuerySchemaUtil.getFieldName(info)).isEqualTo("info");
        } finally {
            Locale.setDefault(previous);
        }
    }

    /**
     * The lower-casing changes no field the library would derive under an English locale: the same
     * names, numbers, labels, types and options, nested fields included. Only a nested message
     * type's name, which the library takes from the column name as handed over, is lower-case
     * ({@code root__items}, not {@code root__Items}); a row is encoded by field number, and no
     * checkpoint stores a descriptor.
     */
    @Test
    void derivesTheLibrarysOwnFieldsUnderAnEnglishDefaultLocale() throws Exception {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.US);
            Descriptors.Descriptor derived = RowDescriptors.derive(mixedCase(), "the schema");
            Descriptors.Descriptor libraryAlone =
                    BQTableSchemaToProtoDescriptor.convertBQTableSchemaToProtoDescriptor(
                            mixedCase());

            assertThat(fields(derived)).isEqualTo(fields(libraryAlone));
            assertThat(derived.getFields().get(1).getMessageType().getName())
                    .isEqualTo("root__items");
            assertThat(libraryAlone.getFields().get(1).getMessageType().getName())
                    .isEqualTo("root__Items");
        } finally {
            Locale.setDefault(previous);
        }
    }

    /** Every field of the descriptor, nested ones included, without the message type names. */
    private static String fields(Descriptors.Descriptor descriptor) {
        StringBuilder fields = new StringBuilder("{");
        for (Descriptors.FieldDescriptor field : descriptor.getFields()) {
            fields.append(field.toProto().toBuilder().clearTypeName().build()).append(';');
            if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                fields.append(fields(field.getMessageType()));
            }
        }
        return fields.append('}').toString();
    }

    /** The descriptor is the same under every default locale, so the client's is the TM's. */
    @Test
    void derivesTheSameDescriptorUnderEveryDefaultLocale() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(Locale.US);
            DescriptorProtos.DescriptorProto english =
                    RowDescriptors.derive(mixedCase(), "the schema").toProto();
            Locale.setDefault(new Locale("tr", "TR"));
            DescriptorProtos.DescriptorProto turkish =
                    RowDescriptors.derive(mixedCase(), "the schema").toProto();

            assertThat(turkish).isEqualTo(english);
        } finally {
            Locale.setDefault(previous);
        }
    }

    /** Lower-casing keeps everything but the name, so a range keeps its element type. */
    @Test
    void keepsARangeColumnsElementTypeWhenItLowerCases() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            TableSchema withRange =
                    TableSchema.newBuilder()
                            .addFields(
                                    field("Interval", TableFieldSchema.Type.RANGE).toBuilder()
                                            .setRangeElementType(
                                                    TableFieldSchema.FieldElementType.newBuilder()
                                                            .setType(TableFieldSchema.Type.DATE))
                                            .build())
                            .build();

            Descriptors.Descriptor descriptor = RowDescriptors.derive(withRange, "the schema");

            assertThat(descriptor.getFields())
                    .extracting(Descriptors.FieldDescriptor::getName)
                    .containsExactly("interval");
            assertThat(descriptor.getFields().get(0).getMessageType().getFields())
                    .extracting(Descriptors.FieldDescriptor::getName)
                    .containsExactly("start", "end");
        } finally {
            Locale.setDefault(previous);
        }
    }

    private static TableFieldSchema struct(String name, TableFieldSchema... fields) {
        TableFieldSchema.Builder builder = field(name, TableFieldSchema.Type.STRUCT).toBuilder();
        for (TableFieldSchema field : fields) {
            builder.addFields(field);
        }
        return builder.build();
    }

    private static TableFieldSchema int64(String name) {
        return field(name, TableFieldSchema.Type.INT64);
    }

    private static TableFieldSchema range(String name, TableFieldSchema.Type element) {
        return field(name, TableFieldSchema.Type.RANGE).toBuilder()
                .setRangeElementType(
                        TableFieldSchema.FieldElementType.newBuilder().setType(element))
                .build();
    }

    /**
     * The library names a nested type after its column's path joined with {@code __}, so {@code
     * a__b} beside {@code a.b} get one name; measured against BigQuery, the Storage Write API then
     * swapped the values of {@code a.b} silently (#2192).
     */
    @Test
    void refusesColumnsWhoseNestedTypesWouldShareANameWithDifferentFields() {
        TableSchema colliding =
                TableSchema.newBuilder()
                        .addFields(struct("a__b", int64("p"), int64("q")))
                        .addFields(struct("a", struct("b", int64("q"), int64("p"))))
                        .build();

        assertThatThrownBy(() -> RowDescriptors.derive(colliding, "com.example.Record"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Failed to derive a BigQuery-storage compatible descriptor for"
                                + " com.example.Record: the columns a__b and a.b would share the"
                                + " protobuf message type root__a__b with different fields; rename"
                                + " one of them");
    }

    /**
     * Two types with one name make the library resolve a field to either of them, so the descriptor
     * alone gave {@code a.b} the fields of {@code a__b} in about half of all runs; the check reads
     * every type the library built, so it refuses every run.
     */
    @Test
    void refusesACollisionTheLibraryResolvesEitherWay() {
        TableSchema colliding =
                TableSchema.newBuilder()
                        .addFields(struct("a__b", int64("x")))
                        .addFields(
                                struct(
                                        "a",
                                        struct("b", field("y", TableFieldSchema.Type.STRING)),
                                        struct("c", int64("x"))))
                        .build();

        for (int run = 0; run < 50; run++) {
            assertThatThrownBy(() -> RowDescriptors.derive(colliding, "the schema"))
                    .as("run %s", run)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("the columns a__b and a.b would share");
        }
    }

    /**
     * The library names a column whose lower-cased name is not protobuf-compatible with a
     * placeholder, its type too, so the message finds that column by the same rule.
     */
    @Test
    void namesBothColumnsWhenOneTypeIsNamedAfterAPlaceholder() {
        TableSchema colliding =
                TableSchema.newBuilder()
                        .addFields(struct("a__col_w6k", int64("x")))
                        .addFields(
                                struct(
                                        "a",
                                        struct("\u00E9", field("y", TableFieldSchema.Type.STRING))))
                        .build();

        assertThatThrownBy(() -> RowDescriptors.derive(colliding, "the schema"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("the columns a__col_w6k and a.\u00E9 would share");
    }

    @Test
    void refusesRangeColumnsWhoseTypesWouldShareANameWithDifferentFields() {
        TableSchema colliding =
                TableSchema.newBuilder()
                        .addFields(range("a__b", TableFieldSchema.Type.DATE))
                        .addFields(struct("a", range("b", TableFieldSchema.Type.TIMESTAMP)))
                        .build();

        assertThatThrownBy(() -> RowDescriptors.derive(colliding, "the schema"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("the columns a__b and a.b would share");
    }

    /**
     * One name with identical fields is harmless: the descriptor and the writer schema are the same
     * whichever type a field resolves to. That holds whether the library shares one type, for
     * identical column lists, or builds two, for columns differing only in a description, a
     * precision or the case of a name.
     */
    @Test
    void acceptsColumnsWhoseSharedTypeNameHasIdenticalFields() {
        TableSchema identical =
                TableSchema.newBuilder()
                        .addFields(struct("a__b", int64("p"), int64("q")))
                        .addFields(struct("a", struct("b", int64("p"), int64("q"))))
                        .build();
        TableSchema describedApart =
                TableSchema.newBuilder()
                        .addFields(
                                struct(
                                        "a__b",
                                        int64("x").toBuilder().setDescription("one").build(),
                                        field("n", TableFieldSchema.Type.NUMERIC).toBuilder()
                                                .setPrecision(10)
                                                .setScale(2)
                                                .build()))
                        .addFields(
                                struct(
                                        "a",
                                        struct(
                                                "b",
                                                int64("X"),
                                                field("n", TableFieldSchema.Type.NUMERIC)
                                                        .toBuilder()
                                                        .setPrecision(12)
                                                        .setScale(2)
                                                        .build())))
                        .build();

        assertThat(RowDescriptors.derive(identical, "the schema").getFields()).hasSize(2);
        assertThat(RowDescriptors.derive(describedApart, "the schema").getFields()).hasSize(2);
    }

    /**
     * {@code a__B} beside {@code a.b} differ only in case, and the names reach the library
     * lower-cased under every locale, so they collide under every locale: a client and a task
     * manager whose default locales differ refuse the same schemas.
     */
    @Test
    void refusesTypeNamesThatDifferOnlyInCaseUnderEveryDefaultLocale() {
        TableSchema folding =
                TableSchema.newBuilder()
                        .addFields(struct("a__B", int64("x"), int64("y")))
                        .addFields(struct("a", struct("b", int64("y"), int64("x"))))
                        .build();
        Locale previous = Locale.getDefault();
        try {
            for (Locale locale : new Locale[] {Locale.US, new Locale("tr", "TR")}) {
                Locale.setDefault(locale);
                assertThatThrownBy(() -> RowDescriptors.derive(folding, "the schema"))
                        .as("under %s", locale)
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("the columns a__B and a.b would share");
            }
        } finally {
            Locale.setDefault(previous);
        }
    }

    /** Under a Turkish locale "ID" and "id" now collide, as they always did everywhere else. */
    @Test
    void columnsDifferingOnlyByCaseCollideUnderATurkishDefaultLocaleToo() {
        Locale previous = Locale.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            TableSchema ambiguous =
                    TableSchema.newBuilder()
                            .addFields(field("ID", TableFieldSchema.Type.INT64))
                            .addFields(field("id", TableFieldSchema.Type.INT64))
                            .build();

            assertThatThrownBy(() -> RowDescriptors.derive(ambiguous, "com.example.Record"))
                    .isInstanceOf(IllegalStateException.class)
                    .cause()
                    .isInstanceOf(Descriptors.DescriptorValidationException.class);
        } finally {
            Locale.setDefault(previous);
        }
    }

    @Test
    void namesWhatTheDescriptorWasDerivedFromWhenValidationFails() {
        // Two columns that differ only in case: BigQuery has no such table, but a schema handed to
        // the JSON serializer is not read back from one, and the descriptor generated from it
        // holds the same field name twice.
        TableSchema ambiguous =
                TableSchema.newBuilder()
                        .addFields(field("count", TableFieldSchema.Type.INT64))
                        .addFields(field("COUNT", TableFieldSchema.Type.INT64))
                        .build();

        assertThatThrownBy(() -> RowDescriptors.derive(ambiguous, "com.example.Record"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Failed to derive a BigQuery-storage compatible descriptor for"
                                + " com.example.Record")
                .cause()
                .isInstanceOf(Descriptors.DescriptorValidationException.class);
    }

    @Test
    void leavesWhatTheLibraryRefusesToMapAsItReportsIt() {
        // A RANGE column without its element type. The library maps RANGE itself, so this is a
        // schema it rejects rather than a type it cannot express — which is the point: whatever
        // it rejects, it rejects with an unchecked exception of its own, and that one is passed
        // through rather than relabelled.
        TableSchema unmappable =
                TableSchema.newBuilder()
                        .addFields(field("window", TableFieldSchema.Type.RANGE))
                        .build();

        assertThatThrownBy(() -> RowDescriptors.derive(unmappable, "com.example.Record"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RANGE requires range element type");
    }
}
