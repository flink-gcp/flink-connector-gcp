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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.table.sink.PubSubDynamicSink;
import io.github.flink.gcp.connector.pubsub.table.sink.PublisherOptionsMapper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@code pubsub} table connector factory.
 *
 * <p>Only {@code project} and {@code format} are required. The destination of each direction —
 * {@code topic} for a sink — is checked in the {@code create...} method that needs it rather than
 * declared required, because one factory serves both reading and writing and a table used for only
 * one of them must not be forced to configure the other.
 */
@Internal
public class PubSubDynamicTableFactory implements DynamicTableSinkFactory {

    /** The value of {@code 'connector'} that selects this factory. */
    public static final String IDENTIFIER = "pubsub";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>(Arrays.asList(PubSubConnectorOptions.PROJECT, FactoryUtil.FORMAT));
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(
                Arrays.asList(
                        PubSubConnectorOptions.EMULATOR_ENDPOINT,
                        PubSubConnectorOptions.TOPIC,
                        PubSubConnectorOptions.SINK_CREATE_DISPOSITION,
                        PubSubConnectorOptions.SINK_BATCHING_ELEMENT_COUNT_THRESHOLD,
                        PubSubConnectorOptions.SINK_BATCHING_REQUEST_BYTE_THRESHOLD,
                        PubSubConnectorOptions.SINK_BATCHING_DELAY_THRESHOLD,
                        PubSubConnectorOptions.SINK_RETRY_TOTAL_TIMEOUT,
                        PubSubConnectorOptions.SINK_RETRY_INITIAL_DELAY,
                        PubSubConnectorOptions.SINK_RETRY_DELAY_MULTIPLIER,
                        PubSubConnectorOptions.SINK_RETRY_MAX_DELAY,
                        PubSubConnectorOptions.SINK_RETRY_INITIAL_RPC_TIMEOUT,
                        PubSubConnectorOptions.SINK_RETRY_RPC_TIMEOUT_MULTIPLIER,
                        PubSubConnectorOptions.SINK_RETRY_MAX_RPC_TIMEOUT,
                        PubSubConnectorOptions.SINK_RETRY_MAX_ATTEMPTS,
                        PubSubConnectorOptions.SINK_MESSAGE_ORDERING_ENABLED,
                        PubSubConnectorOptions.SINK_IN_FLIGHT_MAX_MESSAGES,
                        PubSubConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES,
                        PubSubConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                        PubSubConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                        PubSubConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
                        FactoryUtil.SINK_PARALLELISM));
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        EncodingFormat<SerializationSchema<RowData>> encodingFormat =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        ReadableConfig config = helper.getOptions();
        String topic =
                config.getOptional(PubSubConnectorOptions.TOPIC)
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                String.format(
                                                        "Option '%s' is required to write to a"
                                                                + " '%s' table.",
                                                        PubSubConnectorOptions.TOPIC.key(),
                                                        IDENTIFIER)));
        return new PubSubDynamicSink(
                context.getPhysicalRowDataType(),
                encodingFormat,
                TopicDestination.of(config.get(PubSubConnectorOptions.PROJECT), topic),
                config.getOptional(PubSubConnectorOptions.SINK_CREATE_DISPOSITION).orElse(null),
                PublisherOptionsMapper.map(config),
                config.getOptional(PubSubConnectorOptions.EMULATOR_ENDPOINT).orElse(null),
                config.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }
}
