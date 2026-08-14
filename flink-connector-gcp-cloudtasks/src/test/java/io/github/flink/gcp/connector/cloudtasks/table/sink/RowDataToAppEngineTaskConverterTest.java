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

package io.github.flink.gcp.connector.cloudtasks.table.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.cloudtasks.table.CloudTasksConnectorOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowDataToAppEngineTaskConverter}. */
class RowDataToAppEngineTaskConverterTest {

    private static StringData str(String value) {
        return StringData.fromString(value);
    }

    private static GenericMapData headers(String firstName, String secondName) {
        Map<StringData, StringData> data = new LinkedHashMap<>();
        data.put(str(firstName), str("first"));
        data.put(str(secondName), str("second"));
        return new GenericMapData(data);
    }

    @Test
    void emptyRoutingMetadataClearsEveryFixedSelector() throws Exception {
        Configuration config = new Configuration();
        config.set(CloudTasksConnectorOptions.APP_ENGINE_RELATIVE_URI, "/tasks");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_SERVICE, "worker");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_VERSION, "v2");
        config.set(CloudTasksConnectorOptions.APP_ENGINE_INSTANCE, "instance-3");
        RowDataToAppEngineTaskConverter converter =
                new RowDataToAppEngineTaskConverter(
                        0,
                        new WritableMetadata[] {
                            WritableMetadata.APP_ENGINE_SERVICE,
                            WritableMetadata.APP_ENGINE_VERSION,
                            WritableMetadata.APP_ENGINE_INSTANCE
                        },
                        AppEngineTargetSpec.from(config, null));

        Task task = converter.convert(GenericRowData.of(str(""), str(""), str(""))).build();

        assertThat(task.getAppEngineHttpRequest().hasAppEngineRouting()).isFalse();
    }

    @Test
    void dynamicRelativeUriUsesTheSharedAppEngineValidation() {
        Configuration config = new Configuration();
        RowDataToAppEngineTaskConverter converter =
                new RowDataToAppEngineTaskConverter(
                        0,
                        new WritableMetadata[] {WritableMetadata.RELATIVE_URI},
                        AppEngineTargetSpec.from(config, null));

        assertThatThrownBy(() -> converter.convert(GenericRowData.of(str("tasks/1"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("must be empty or begin with '/'");
    }

    @Test
    void rowHeadersCannotChooseACaseInsensitiveWinner() {
        Configuration config = new Configuration();
        config.set(CloudTasksConnectorOptions.APP_ENGINE_RELATIVE_URI, "/tasks");
        RowDataToAppEngineTaskConverter converter =
                new RowDataToAppEngineTaskConverter(
                        0,
                        new WritableMetadata[] {WritableMetadata.HEADERS},
                        AppEngineTargetSpec.from(config, null));

        assertThatThrownBy(
                        () ->
                                converter.convert(
                                        GenericRowData.of(headers("X-Request-Id", "x-request-id"))))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("case-insensitive duplicate");
    }
}
