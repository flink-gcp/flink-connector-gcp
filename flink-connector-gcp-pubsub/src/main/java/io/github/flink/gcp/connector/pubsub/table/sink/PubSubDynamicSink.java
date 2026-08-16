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

package io.github.flink.gcp.connector.pubsub.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSink;
import io.github.flink.gcp.connector.pubsub.sink.PubSubSinkBuilder;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A table sink publishing rows to one Pub/Sub topic.
 *
 * <p>A thin mapping onto {@link PubSubSink}: the format encodes the physical columns into the
 * message payload, and the {@link WritableMetadata} columns become the message's attributes and
 * ordering key.
 *
 * <p>Metadata is <b>not</b> forwarded to the format. No format ships writable metadata today, and
 * the Kafka connector does not forward either, so the physical prefix of a consumed row is exactly
 * the table's physical columns.
 */
@Internal
public final class PubSubDynamicSink implements DynamicTableSink, SupportsWritingMetadata {

    private final DataType physicalDataType;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final TopicDestination topic;
    @Nullable private final CreateDisposition createDisposition;
    @Nullable private final TopicCreateOptions topicCreateOptions;
    private final PubSubPublisherOptions publisherOptions;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final Integer parallelism;

    /** Metadata keys the planner selected, in {@link WritableMetadata#listAll()} order. */
    private List<String> metadataKeys = Collections.emptyList();

    /**
     * Builds a sink from values the factory has already resolved.
     *
     * <p>Deliberately takes no {@code ReadableConfig}: turning a DDL option into a value happens in
     * one place, the factory, so this class has no configuration vocabulary at all.
     *
     * @param physicalDataType the row type of the table's physical columns
     * @param encodingFormat the format encoding the payload
     * @param topic the destination topic
     * @param createDisposition whether a missing topic may be created, or {@code null} to leave the
     *     sink's own default
     * @param topicCreateOptions the settings a created topic takes, or {@code null} for service
     *     defaults
     * @param publisherOptions the publisher and writer tuning
     * @param emulatorEndpoint the emulator to use instead of the service, or {@code null}
     * @param parallelism the sink operator's parallelism, or {@code null} for the job's
     */
    public PubSubDynamicSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            TopicDestination topic,
            @Nullable CreateDisposition createDisposition,
            @Nullable TopicCreateOptions topicCreateOptions,
            PubSubPublisherOptions publisherOptions,
            @Nullable String emulatorEndpoint,
            @Nullable Integer parallelism) {
        this(
                physicalDataType,
                encodingFormat,
                topic,
                createDisposition,
                topicCreateOptions,
                publisherOptions,
                null,
                emulatorEndpoint,
                parallelism);
    }

    /** Builds a sink with an optional service-account key-file path. */
    public PubSubDynamicSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            TopicDestination topic,
            @Nullable CreateDisposition createDisposition,
            @Nullable TopicCreateOptions topicCreateOptions,
            PubSubPublisherOptions publisherOptions,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            @Nullable Integer parallelism) {
        this.physicalDataType =
                Preconditions.checkNotNull(physicalDataType, "physicalDataType must not be null");
        this.encodingFormat =
                Preconditions.checkNotNull(encodingFormat, "encodingFormat must not be null");
        this.topic = Preconditions.checkNotNull(topic, "topic must not be null");
        this.createDisposition = createDisposition;
        this.topicCreateOptions = topicCreateOptions;
        this.publisherOptions =
                Preconditions.checkNotNull(publisherOptions, "publisherOptions must not be null");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.parallelism = parallelism;
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return WritableMetadata.listAll();
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        if (metadataKeys.contains(WritableMetadata.ORDERING_KEY.getKey())
                && !publisherOptions.isEnableMessageOrdering()) {
            throw new ValidationException(
                    String.format(
                            "Writing the '%s' metadata column requires '%s' to be true. Without it"
                                    + " the sink rejects every message carrying an ordering key, so"
                                    + " the job would fail on its first record.",
                            WritableMetadata.ORDERING_KEY.getKey(),
                            PubSubConnectorOptions.SINK_MESSAGE_ORDERING_ENABLED.key()));
        }
        this.metadataKeys = metadataKeys;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // Pub/Sub has no way to express a retraction, so an updating query must be rejected at
        // plan time rather than silently publishing its -U and -D rows as ordinary messages.
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        WritableMetadata[] selected =
                metadataKeys.stream().map(WritableMetadata::of).toArray(WritableMetadata[]::new);
        SerializationSchema<RowData> encoder =
                encodingFormat.createRuntimeEncoder(context, physicalDataType);
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(
                        encoder, DataType.getFieldCount(physicalDataType), selected);

        PubSubSinkBuilder<RowData> builder =
                PubSubSink.<RowData>builder()
                        .topic(topic)
                        .serializer(serializer)
                        .publisherOptions(publisherOptions);
        if (createDisposition != null) {
            builder.createDisposition(createDisposition);
        }
        if (topicCreateOptions != null) {
            builder.topicCreateOptions(topicCreateOptions);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        Sink<RowData> sink = builder.build();
        if (metadataKeys.contains(WritableMetadata.ORDERING_KEY.getKey())) {
            int orderingKeyIndex =
                    DataType.getFieldCount(physicalDataType)
                            + metadataKeys.indexOf(WritableMetadata.ORDERING_KEY.getKey());
            return new OrderingKeyDataStreamSinkProvider(sink, orderingKeyIndex, parallelism);
        }
        return SinkV2Provider.of(sink, parallelism);
    }

    @Override
    public DynamicTableSink copy() {
        PubSubDynamicSink copy =
                new PubSubDynamicSink(
                        physicalDataType,
                        encodingFormat,
                        topic,
                        createDisposition,
                        topicCreateOptions,
                        publisherOptions,
                        serviceAccountKeyFile,
                        emulatorEndpoint,
                        parallelism);
        copy.metadataKeys = metadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Pub/Sub table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PubSubDynamicSink that = (PubSubDynamicSink) o;
        return physicalDataType.equals(that.physicalDataType)
                && encodingFormat.equals(that.encodingFormat)
                && topic.equals(that.topic)
                && createDisposition == that.createDisposition
                && Objects.equals(topicCreateOptions, that.topicCreateOptions)
                && publisherOptions.equals(that.publisherOptions)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(parallelism, that.parallelism)
                && metadataKeys.equals(that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalDataType,
                encodingFormat,
                topic,
                createDisposition,
                topicCreateOptions,
                publisherOptions,
                serviceAccountKeyFile,
                emulatorEndpoint,
                parallelism,
                metadataKeys);
    }

    /** Routes equal ordering keys to one sink writer before attaching the Pub/Sub sink. */
    private static final class OrderingKeyDataStreamSinkProvider implements DataStreamSinkProvider {

        private final Sink<RowData> sink;
        private final int orderingKeyIndex;
        @Nullable private final Integer parallelism;

        private OrderingKeyDataStreamSinkProvider(
                Sink<RowData> sink, int orderingKeyIndex, @Nullable Integer parallelism) {
            this.sink = sink;
            this.orderingKeyIndex = orderingKeyIndex;
            this.parallelism = parallelism;
        }

        @Override
        public DataStreamSink<?> consumeDataStream(
                ProviderContext providerContext, DataStream<RowData> dataStream) {
            int writerParallelism =
                    parallelism != null
                            ? parallelism
                            : dataStream.getExecutionEnvironment().getParallelism();
            DataStream<RowData> routed =
                    writerParallelism == 1
                            ? dataStream
                            : dataStream.keyBy(new OrderingKeySelector(orderingKeyIndex));
            DataStreamSink<RowData> attached = routed.sinkTo(sink).name("Pub/Sub table sink");
            providerContext.generateUid("pubsub-sink").ifPresent(attached::uid);
            if (parallelism != null) {
                attached.setParallelism(parallelism);
            }
            return attached;
        }

        @Override
        public Optional<Integer> getParallelism() {
            return Optional.ofNullable(parallelism);
        }
    }

    /** Selects stable keys for ordered messages and distinct keys for unkeyed messages. */
    static final class OrderingKeySelector implements KeySelector<RowData, String> {

        private static final long serialVersionUID = 1L;

        private final int orderingKeyIndex;
        private long unkeyedSequence;

        OrderingKeySelector(int orderingKeyIndex) {
            this.orderingKeyIndex = orderingKeyIndex;
        }

        @Override
        public String getKey(RowData row) {
            if (row.isNullAt(orderingKeyIndex)) {
                return nextUnkeyedRoutingKey();
            }
            String orderingKey = row.getString(orderingKeyIndex).toString();
            if (orderingKey.isEmpty()) {
                return nextUnkeyedRoutingKey();
            }
            return "ordered:" + orderingKey;
        }

        private String nextUnkeyedRoutingKey() {
            return "unkeyed:" + unkeyedSequence++;
        }
    }
}
