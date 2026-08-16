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

package io.github.flink.gcp.connector.bigquery.sink.serializer.avro;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AvroSchemaOptions}. */
class AvroSchemaOptionsTest {

    @Test
    void defaultsMapNothingAndConstrainNothing() {
        AvroSchemaOptions options = AvroSchemaOptions.defaults();

        assertThat(options.getJsonFieldPaths()).isEmpty();
        assertThat(options.getGeographyFieldPaths()).isEmpty();
        assertThat(options.isDeriveRequiredColumns()).isFalse();
        assertThat(options.isJsonField("anything")).isFalse();
        assertThat(options.isGeographyField("anything")).isFalse();
        assertThat(options.markedType("anything")).isNull();
    }

    @Test
    void geographyFieldPathsAccumulateAcrossCalls() {
        AvroSchemaOptions options =
                AvroSchemaOptions.builder()
                        .geographyFieldPath("a")
                        .geographyFieldPaths(Arrays.asList("b.c", "d"))
                        .geographyFieldPath("a")
                        .build();

        assertThat(options.getGeographyFieldPaths()).containsExactlyInAnyOrder("a", "b.c", "d");
        assertThat(options.isGeographyField("b.c")).isTrue();
        assertThat(options.isGeographyField("b")).isFalse();
        // Independent of the JSON marker, which shares none of its state.
        assertThat(options.isJsonField("a")).isFalse();
    }

    @Test
    void markedTypeNamesTheMarkerThatClaimedThePath() {
        AvroSchemaOptions options =
                AvroSchemaOptions.builder()
                        .jsonFieldPath("payload")
                        .geographyFieldPath("boundary")
                        .build();

        assertThat(options.markedType("payload")).isEqualTo(TableFieldSchema.Type.JSON);
        assertThat(options.markedType("boundary")).isEqualTo(TableFieldSchema.Type.GEOGRAPHY);
        assertThat(options.markedType("plain")).isNull();
    }

    /** A column has one type, so a path claimed by both markers is a configuration error. */
    @Test
    void markedTypeRejectsAPathClaimedByBothMarkers() {
        AvroSchemaOptions options =
                AvroSchemaOptions.builder().jsonFieldPath("f").geographyFieldPath("f").build();

        assertThatThrownBy(() -> options.markedType("f"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("both a JSON and a GEOGRAPHY");
    }

    @Test
    void jsonFieldPathsAccumulateAcrossCalls() {
        AvroSchemaOptions options =
                AvroSchemaOptions.builder()
                        .jsonFieldPath("a")
                        .jsonFieldPaths(Arrays.asList("b.c", "d"))
                        .jsonFieldPath("a")
                        .build();

        assertThat(options.getJsonFieldPaths()).containsExactlyInAnyOrder("a", "b.c", "d");
        assertThat(options.isJsonField("b.c")).isTrue();
        assertThat(options.isJsonField("b")).isFalse();
    }

    @Test
    void deriveRequiredColumnsIsOptIn() {
        assertThat(AvroSchemaOptions.builder().build().isDeriveRequiredColumns()).isFalse();
        assertThat(
                        AvroSchemaOptions.builder()
                                .deriveRequiredColumns()
                                .build()
                                .isDeriveRequiredColumns())
                .isTrue();
    }

    @Test
    void returnedPathsAreUnmodifiable() {
        AvroSchemaOptions options = AvroSchemaOptions.builder().jsonFieldPath("a").build();

        assertThatThrownBy(() -> options.getJsonFieldPaths().add("b"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> options.getGeographyFieldPaths().add("b"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void buildingTwiceFromOneBuilderDoesNotShareState() {
        AvroSchemaOptions.Builder builder =
                AvroSchemaOptions.builder().jsonFieldPath("a").geographyFieldPath("g");
        AvroSchemaOptions first = builder.build();
        builder.jsonFieldPath("b").geographyFieldPath("h");

        assertThat(first.getJsonFieldPaths()).containsExactly("a");
        assertThat(first.getGeographyFieldPaths()).containsExactly("g");
        assertThat(builder.build().getJsonFieldPaths()).containsExactlyInAnyOrder("a", "b");
        assertThat(builder.build().getGeographyFieldPaths()).containsExactlyInAnyOrder("g", "h");
    }

    @Test
    void rejectsNullPaths() {
        assertThatThrownBy(() -> AvroSchemaOptions.builder().jsonFieldPath(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AvroSchemaOptions.builder().jsonFieldPaths(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AvroSchemaOptions.builder().geographyFieldPath(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> AvroSchemaOptions.builder().geographyFieldPaths(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        AvroSchemaOptions options =
                AvroSchemaOptions.builder()
                        .jsonFieldPath("a.b")
                        .geographyFieldPath("c.d")
                        .deriveRequiredColumns()
                        .build();

        AvroSchemaOptions copy = InstantiationUtil.clone(options);

        assertThat(copy.getJsonFieldPaths()).containsExactly("a.b");
        assertThat(copy.getGeographyFieldPaths()).containsExactly("c.d");
        assertThat(copy.isDeriveRequiredColumns()).isTrue();
    }
}
