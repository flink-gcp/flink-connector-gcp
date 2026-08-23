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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.pubsub.source.DeserializationFailurePolicy;
import io.github.flink.gcp.connector.pubsub.source.OrderingMode;
import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceBuilder;
import io.github.flink.gcp.connector.pubsub.source.PubSubStartPosition;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionCreateOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A table source consuming rows from one or more Pub/Sub subscriptions.
 *
 * <p>A thin mapping onto {@link PubSubSource}: the format decodes the message payload into the
 * physical columns, and the {@link ReadableMetadata} columns are appended from the delivery.
 *
 * <p>Format metadata <b>is</b> forwarded, unprefixed. Kafka prefixes its with {@code value.} only
 * because it has a key format to disambiguate against; there is one format here, so a bare name is
 * unambiguous — and a collision between a format key and a connector key is rejected rather than
 * silently resolved.
 */
@Internal
public final class PubSubDynamicSource implements ScanTableSource, SupportsReadingMetadata {

    private final DataType physicalDataType;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private final List<SubscriptionDestination> subscriptions;
    private final Map<SubscriptionDestination, SubscriptionCreateOptions> createOptions;
    @Nullable private final PubSubStartPosition startPosition;
    @Nullable private final OrderingMode orderingMode;
    @Nullable private final DeserializationFailurePolicy deserializationFailurePolicy;
    private final PubSubSubscriberOptions subscriberOptions;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final Integer parallelism;

    /** The row type the source produces, physical columns plus whatever metadata was selected. */
    private DataType producedDataType;

    /** This connector's metadata keys the planner selected, in {@link ReadableMetadata} order. */
    private List<String> metadataKeys = Collections.emptyList();

    /**
     * Builds a source from values the factory has already resolved.
     *
     * @param physicalDataType the row type of the table's physical columns
     * @param decodingFormat the format decoding the payload
     * @param subscriptions the subscriptions to consume, at least one
     * @param createOptions the settings each missing subscription is created with, keyed by every
     *     subscription, or an empty map to require all subscriptions to exist
     * @param startPosition where to start consuming, or {@code null} to leave the source's own
     *     default
     * @param orderingMode the ordering mode, or {@code null} to leave the source's own default
     * @param deserializationFailurePolicy what to do with an undecodable message, or {@code null}
     *     for the source's own default
     * @param subscriberOptions the subscriber tuning
     * @param serviceAccountKeyFile the service-account JSON key path each reader and the enumerator
     *     load, or {@code null} for application-default credentials
     * @param emulatorEndpoint the emulator to use instead of the service, or {@code null}
     * @param parallelism the source operator's parallelism, or {@code null} for the job's
     */
    public PubSubDynamicSource(
            DataType physicalDataType,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            List<SubscriptionDestination> subscriptions,
            Map<SubscriptionDestination, SubscriptionCreateOptions> createOptions,
            @Nullable PubSubStartPosition startPosition,
            @Nullable OrderingMode orderingMode,
            @Nullable DeserializationFailurePolicy deserializationFailurePolicy,
            PubSubSubscriberOptions subscriberOptions,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            @Nullable Integer parallelism) {
        this.physicalDataType =
                Preconditions.checkNotNull(physicalDataType, "physicalDataType must not be null");
        this.decodingFormat =
                Preconditions.checkNotNull(decodingFormat, "decodingFormat must not be null");
        this.subscriptions =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Preconditions.checkNotNull(
                                        subscriptions, "subscriptions must not be null")));
        this.createOptions =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                Preconditions.checkNotNull(
                                        createOptions, "createOptions must not be null")));
        Set<SubscriptionDestination> distinctSubscriptions = new HashSet<>(this.subscriptions);
        Preconditions.checkArgument(
                this.createOptions.isEmpty()
                        || (this.createOptions.size() == distinctSubscriptions.size()
                                && this.createOptions.keySet().equals(distinctSubscriptions)),
                "Creation settings must be empty or keyed by every subscription, but subscriptions"
                        + " were %s and creation settings were keyed by %s.",
                this.subscriptions,
                this.createOptions.keySet());
        this.startPosition = startPosition;
        this.orderingMode = orderingMode;
        this.deserializationFailurePolicy = deserializationFailurePolicy;
        this.subscriberOptions =
                Preconditions.checkNotNull(subscriberOptions, "subscriberOptions must not be null");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.parallelism = parallelism;
        this.producedDataType = physicalDataType;
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        Map<String, DataType> formatMetadata = decodingFormat.listReadableMetadata();
        Map<String, DataType> metadata = new LinkedHashMap<>(formatMetadata);
        // Format keys first, connector keys second: the planner hands the selected keys back in
        // this iteration order and lays the produced row out from it, and the format appends its
        // own metadata to the rows it emits — so the connector's must come after.
        for (Map.Entry<String, DataType> entry : ReadableMetadata.listAll().entrySet()) {
            if (formatMetadata.containsKey(entry.getKey())) {
                throw new ValidationException(
                        String.format(
                                "The format declares a readable metadata key '%s', which this"
                                        + " connector also declares. Rename the format's key, or"
                                        + " use a format that does not declare it — resolving the"
                                        + " collision silently would make the column's meaning"
                                        + " depend on the format.",
                                entry.getKey()));
            }
            metadata.put(entry.getKey(), entry.getValue());
        }
        return metadata;
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        List<String> formatKeys = new ArrayList<>();
        List<String> connectorKeys = new ArrayList<>();
        for (String key : metadataKeys) {
            (ReadableMetadata.find(key) != null ? connectorKeys : formatKeys).add(key);
        }
        if (!decodingFormat.listReadableMetadata().isEmpty()) {
            // Guarded on the format *declaring* metadata rather than on the planner having
            // selected some, which is how Kafka does it. Both dodge the reason the guard exists —
            // DecodingFormat.applyReadableMetadata throws by default and no built-in format
            // overrides it, so an unconditional call breaks every table with any metadata column —
            // but only this form can shrink the format's key set back, and the ability's javadoc
            // says the planner may call this more than once.
            decodingFormat.applyReadableMetadata(formatKeys);
        }
        this.metadataKeys = connectorKeys;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        // Delegated rather than hard-coded to insert-only: a changelog format such as debezium-json
        // over Pub/Sub is a legitimate pipeline, and the transport being at-least-once is a
        // property the documentation states rather than something to forbid here.
        return decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        DeserializationSchema<RowData> physical =
                decodingFormat.createRuntimeDecoder(context, physicalDataType);
        ReadableMetadata[] selected =
                metadataKeys.stream().map(ReadableMetadata::of).toArray(ReadableMetadata[]::new);
        TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(producedDataType);

        PubSubSourceBuilder<RowData> builder =
                PubSubSource.<RowData>builder()
                        .deserializer(
                                new RowDataDeserializationSchema(
                                        physical, selected, producedTypeInfo))
                        .subscriberOptions(subscriberOptions);
        if (createOptions.isEmpty()) {
            builder.subscriptions(subscriptions);
        } else {
            // Iterating the subscription list preserves the user's split order while the map gives
            // every destination its own topic binding.
            for (SubscriptionDestination subscription : subscriptions) {
                builder.subscription(subscription, createOptions.get(subscription));
            }
        }
        if (startPosition != null) {
            builder.startPosition(startPosition);
        }
        if (orderingMode != null) {
            builder.orderingMode(orderingMode);
        }
        if (deserializationFailurePolicy != null) {
            builder.deserializationFailurePolicy(deserializationFailurePolicy);
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        Source<RowData, ?, ?> source = builder.build();
        return SourceProvider.of(source, parallelism);
    }

    @Override
    public DynamicTableSource copy() {
        PubSubDynamicSource copy =
                new PubSubDynamicSource(
                        physicalDataType,
                        decodingFormat,
                        subscriptions,
                        createOptions,
                        startPosition,
                        orderingMode,
                        deserializationFailurePolicy,
                        subscriberOptions,
                        serviceAccountKeyFile,
                        emulatorEndpoint,
                        parallelism);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Pub/Sub table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PubSubDynamicSource that = (PubSubDynamicSource) o;
        return physicalDataType.equals(that.physicalDataType)
                && decodingFormat.equals(that.decodingFormat)
                && subscriptions.equals(that.subscriptions)
                && createOptions.equals(that.createOptions)
                && Objects.equals(startPosition, that.startPosition)
                && orderingMode == that.orderingMode
                && deserializationFailurePolicy == that.deserializationFailurePolicy
                && subscriberOptions.equals(that.subscriberOptions)
                && Objects.equals(serviceAccountKeyFile, that.serviceAccountKeyFile)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(parallelism, that.parallelism)
                && producedDataType.equals(that.producedDataType)
                && metadataKeys.equals(that.metadataKeys);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                physicalDataType,
                decodingFormat,
                subscriptions,
                createOptions,
                startPosition,
                orderingMode,
                deserializationFailurePolicy,
                subscriberOptions,
                serviceAccountKeyFile,
                emulatorEndpoint,
                parallelism,
                producedDataType,
                metadataKeys);
    }
}
