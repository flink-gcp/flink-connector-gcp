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

package io.github.flink.gcp.connector.cloudtasks.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.table.sink.CloudTasksDynamicSink;
import io.github.flink.gcp.connector.cloudtasks.table.sink.CloudTasksWriterOptionsMapper;
import io.github.flink.gcp.connector.cloudtasks.table.sink.TableHttpTarget;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Factory for the sink-only {@code cloud-tasks} table connector. */
@Internal
public class CloudTasksDynamicTableFactory implements DynamicTableSinkFactory {

    /** The value of {@code connector} that selects this factory. */
    public static final String IDENTIFIER = "cloud-tasks";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(
                Arrays.asList(
                        CloudTasksConnectorOptions.PROJECT,
                        CloudTasksConnectorOptions.LOCATION,
                        CloudTasksConnectorOptions.QUEUE,
                        FactoryUtil.FORMAT));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        CloudTasksConnectorOptions.HTTP_URL,
                        CloudTasksConnectorOptions.HTTP_METHOD,
                        CloudTasksConnectorOptions.HTTP_HEADERS,
                        CloudTasksConnectorOptions.HTTP_OIDC_SERVICE_ACCOUNT_EMAIL,
                        CloudTasksConnectorOptions.HTTP_OIDC_AUDIENCE,
                        CloudTasksConnectorOptions.HTTP_OAUTH_SERVICE_ACCOUNT_EMAIL,
                        CloudTasksConnectorOptions.HTTP_OAUTH_SCOPE,
                        CloudTasksConnectorOptions.SERVICE_ACCOUNT_KEY_FILE,
                        CloudTasksConnectorOptions.EMULATOR_ENDPOINT,
                        CloudTasksConnectorOptions.SINK_MAX_IN_FLIGHT_TASKS,
                        CloudTasksConnectorOptions.SINK_RETRY_INITIAL_BACKOFF,
                        CloudTasksConnectorOptions.SINK_RETRY_MAX_BACKOFF,
                        CloudTasksConnectorOptions.SINK_RETRY_MAX_ATTEMPTS,
                        CloudTasksConnectorOptions.SINK_NOT_FOUND_RETRY_INITIAL_BACKOFF,
                        CloudTasksConnectorOptions.SINK_NOT_FOUND_RETRY_MAX_BACKOFF,
                        CloudTasksConnectorOptions.SINK_NOT_FOUND_RETRY_MAX_ATTEMPTS,
                        CloudTasksConnectorOptions.SINK_METRICS_PER_DESTINATION,
                        FactoryUtil.SINK_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        validateHeadersSyntax(context);
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        validateCredentials(config);
        return new CloudTasksDynamicSink(
                context.getPhysicalRowDataType(),
                format,
                QueueDestination.of(
                        config.get(CloudTasksConnectorOptions.PROJECT),
                        config.get(CloudTasksConnectorOptions.LOCATION),
                        config.get(CloudTasksConnectorOptions.QUEUE)),
                TableHttpTarget.from(config),
                hasNotNullUrlMetadata(context),
                CloudTasksWriterOptionsMapper.map(config),
                config.getOptional(CloudTasksConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                        .orElse(null),
                config.getOptional(CloudTasksConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    private static boolean hasNotNullUrlMetadata(Context context) {
        for (Column column : context.getCatalogTable().getResolvedSchema().getColumns()) {
            if (column instanceof Column.MetadataColumn) {
                Column.MetadataColumn metadata = (Column.MetadataColumn) column;
                String key = metadata.getMetadataKey().orElse(metadata.getName());
                if ("url".equals(key)
                        && !metadata.isVirtual()
                        && !metadata.getDataType().getLogicalType().isNullable()) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void validateCredentials(ReadableConfig config) {
        String keyFile =
                config.getOptional(CloudTasksConnectorOptions.SERVICE_ACCOUNT_KEY_FILE)
                        .orElse(null);
        if (keyFile != null && keyFile.isBlank()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must not be blank.",
                            CloudTasksConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key()));
        }
        if (keyFile != null
                && config.getOptional(CloudTasksConnectorOptions.EMULATOR_ENDPOINT).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Options '%s' and '%s' cannot be combined: an emulator uses a"
                                    + " plaintext channel with no credentials.",
                            CloudTasksConnectorOptions.SERVICE_ACCOUNT_KEY_FILE.key(),
                            CloudTasksConnectorOptions.EMULATOR_ENDPOINT.key()));
        }
    }

    private static void validateHeadersSyntax(Context context) {
        String option = CloudTasksConnectorOptions.HTTP_HEADERS.key();
        Map<String, String> raw = context.getCatalogTable().getOptions();
        boolean packed = raw.containsKey(option);
        boolean prefixed = raw.keySet().stream().anyMatch(key -> key.startsWith(option + "."));
        if (packed && prefixed) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' must use either packed map syntax or prefixed entries,"
                                    + " not both.",
                            option));
        }
    }
}
