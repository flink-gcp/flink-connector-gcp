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

package io.github.flink.gcp.connector.cloudtasks.table.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import com.google.cloud.tasks.v2.AppEngineHttpRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link RowDataToAppEngineTaskConverter}. */
class RowDataToAppEngineTaskConverterTest {

    private static StringData str(String value) {
        return value == null ? null : StringData.fromString(value);
    }

    private static GenericMapData header(String name, String value) {
        return new GenericMapData(Collections.singletonMap(str(name), str(value)));
    }

    private static RowDataToAppEngineTaskConverter converter(
            Configuration config, WritableMetadata... metadata) {
        return converter(config, null, metadata);
    }

    private static RowDataToAppEngineTaskConverter converter(
            Configuration config, String bodyContentType, WritableMetadata... metadata) {
        return new RowDataToAppEngineTaskConverter(
                0, metadata, AppEngineTargetSpec.from(config, bodyContentType));
    }

    private static Configuration appEngineTarget(String relativeUri) {
        Configuration config = new Configuration();
        config.set(CloudTasksConnectorOptions.APP_ENGINE_RELATIVE_URI, relativeUri);
        return config;
    }

    @Test
    void emptyRoutingMetadataClearsEveryFixedSelector() throws Exception {
        Configuration config = appEngineTarget("/tasks");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_SERVICE, "worker");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_VERSION, "v2");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_INSTANCE, "instance-3");
        RowDataToAppEngineTaskConverter converter =
                converter(
                        config,
                        WritableMetadata.APP_ENGINE_SERVICE,
                        WritableMetadata.APP_ENGINE_VERSION,
                        WritableMetadata.APP_ENGINE_INSTANCE);

        Task task = converter.convert(GenericRowData.of(str(""), str(""), str(""))).build();

        assertThat(task.getAppEngineHttpRequest().hasAppEngineRouting()).isFalse();
    }

    @Test
    void routingMetadataOverridesOnlyTheSelectorsItSelects() throws Exception {
        // A table may declare a column for one routing selector and leave the others to the
        // options. The selected one then comes from the row and the rest fall back, which the
        // all-three selections elsewhere in this module cannot show.
        Configuration config = appEngineTarget("/tasks");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_SERVICE, "fixed-service");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_VERSION, "fixed-version");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_INSTANCE, "fixed-instance");
        RowDataToAppEngineTaskConverter converter =
                converter(config, WritableMetadata.APP_ENGINE_SERVICE);

        Task task = converter.convert(GenericRowData.of(str("row-service"))).build();

        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getService())
                .isEqualTo("row-service");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getVersion())
                .isEqualTo("fixed-version");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getInstance())
                .isEqualTo("fixed-instance");
    }

    @Test
    void routingMetadataWithoutAnyFixedRoutingComesFromTheRowAlone() throws Exception {
        // The unselected selectors fall back to the target's, and a target that configures no
        // routing at all holds none to fall back to. The converter reads all three fallbacks
        // whichever one the table selects, so each has to answer for an absent routing rather
        // than dereference it.
        Configuration config = appEngineTarget("/tasks");
        RowDataToAppEngineTaskConverter converter =
                converter(config, WritableMetadata.APP_ENGINE_SERVICE);

        Task task = converter.convert(GenericRowData.of(str("row-service"))).build();

        // Asserted present first: an absent routing answers getVersion() and getInstance() with
        // the same empty string a present one does, so the two emptiness assertions carry nothing
        // on their own.
        assertThat(task.getAppEngineHttpRequest().hasAppEngineRouting()).isTrue();
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getService())
                .isEqualTo("row-service");
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getVersion()).isEmpty();
        assertThat(task.getAppEngineHttpRequest().getAppEngineRouting().getInstance()).isEmpty();
    }

    @Test
    void dynamicRelativeUriUsesTheSharedAppEngineValidation() {
        RowDataToAppEngineTaskConverter converter =
                converter(new Configuration(), WritableMetadata.RELATIVE_URI);

        assertThatThrownBy(() -> converter.convert(GenericRowData.of(str("tasks/1"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("must be empty or begin with '/'");
    }

    @Test
    void rejectsARelativeUriThatResolvedToNull() {
        // Without the fixed option the metadata is the only source, and the shared validation
        // rejects null too - but with the message of a null argument rather than one naming the
        // two ways to supply the URI, so the phrase is what separates the two rejections.
        RowDataToAppEngineTaskConverter converter =
                converter(new Configuration(), WritableMetadata.RELATIVE_URI);

        assertThatThrownBy(() -> converter.convert(GenericRowData.of(str(null))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("relative URI resolved to null")
                .hasMessageContaining("'app-engine.relative-uri'");
    }

    @Test
    void rowHeadersCannotChooseACaseInsensitiveWinner() {
        Map<StringData, StringData> duplicated = new LinkedHashMap<>();
        duplicated.put(str("X-Request-Id"), str("first"));
        duplicated.put(str("x-request-id"), str("second"));
        RowDataToAppEngineTaskConverter converter =
                converter(appEngineTarget("/tasks"), WritableMetadata.HEADERS);

        assertThatThrownBy(
                        () -> converter.convert(GenericRowData.of(new GenericMapData(duplicated))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("case-insensitive duplicate");
    }

    @Test
    void rejectsARowContentTypeThatConflictsWithTheBodyFormat() {
        RowDataToAppEngineTaskConverter converter =
                converter(
                        appEngineTarget("/tasks"),
                        "application/x-www-form-urlencoded",
                        WritableMetadata.HEADERS);

        assertThatThrownBy(
                        () ->
                                converter.convert(
                                        GenericRowData.of(
                                                header(
                                                        "content-TYPE",
                                                        "application/x-www-form-urlencoded;"
                                                                + " charset=UTF-8"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("App Engine header metadata contains Content-Type")
                .hasMessageContaining("conflicts with the body format's Content-Type");
    }

    @Test
    void leavesAMatchingRowContentTypeForTheBodyFormatToCanonicalize() throws Exception {
        RowDataToAppEngineTaskConverter converter =
                converter(
                        appEngineTarget("/tasks"),
                        "application/x-www-form-urlencoded",
                        WritableMetadata.HEADERS);

        Task task =
                converter
                        .convert(
                                GenericRowData.of(
                                        header(
                                                "content-type",
                                                " APPLICATION/X-WWW-FORM-URLENCODED ")))
                        .build();

        assertThat(task.getAppEngineHttpRequest().getHeadersMap()).isEmpty();
    }

    @Test
    void passesARowContentTypeThroughWhenTheFormatClaimsNone() throws Exception {
        // The negative control for the two conflict cases above: under a generic format the spec's
        // Content-Type is null, and a row is then free to name the media type of the bytes it
        // supplies. Without the converter's null guard, TargetSpec.sameContentType would compare
        // against that null and fail the record with a NullPointerException instead. The HTTP arm
        // has this in RowDataSerializationSchemaTest; App Engine had no equivalent.
        RowDataToAppEngineTaskConverter converter =
                converter(appEngineTarget("/tasks"), WritableMetadata.HEADERS);

        Task task =
                converter
                        .convert(
                                GenericRowData.of(
                                        header("Content-Type", "application/merge-patch+json")))
                        .build();

        assertThat(task.getAppEngineHttpRequest().getHeadersMap())
                .containsOnly(entry("Content-Type", "application/merge-patch+json"));
    }

    @Test
    void aSecondRowStillCarriesTheFixedRequest() throws Exception {
        // The converter builds the fixed request once and merges it into every task, so a record
        // after the first has to find the fixed headers and routing still there and the previous
        // row's headers gone. Those two are the assertable part: the relative URI and the method
        // are re-set from the row-or-option fallback on every call, so they would survive a
        // fixed request that had lost everything.
        Configuration config = appEngineTarget("/fixed");
        config.set(
                CloudTasksConnectorOptions.APP_ENGINE_HEADERS,
                Collections.singletonMap("X-Fixed", "yes"));
        config.set(CloudTasksConnectorOptions.APP_ENGINE_SERVICE, "fixed-service");
        RowDataToAppEngineTaskConverter converter = converter(config, WritableMetadata.HEADERS);

        AppEngineHttpRequest first =
                converter
                        .convert(GenericRowData.of(header("X-Row", "row")))
                        .build()
                        .getAppEngineHttpRequest();
        AppEngineHttpRequest second =
                converter
                        .convert(GenericRowData.of((GenericMapData) null))
                        .build()
                        .getAppEngineHttpRequest();

        assertThat(first.getHeadersMap())
                .containsOnly(entry("X-Fixed", "yes"), entry("X-Row", "row"));
        assertThat(second.getHeadersMap()).containsOnly(entry("X-Fixed", "yes"));
        assertThat(second.getAppEngineRouting().getService()).isEqualTo("fixed-service");
    }
}
