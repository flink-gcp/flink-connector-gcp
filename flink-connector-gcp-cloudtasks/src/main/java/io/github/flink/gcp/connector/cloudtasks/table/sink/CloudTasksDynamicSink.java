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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksWriterOptions;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;
import io.github.flink.gcp.connector.cloudtasks.sink.TaskIdExtractor;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Insert-only table sink creating target tasks in one fixed queue. */
@Internal
public final class CloudTasksDynamicSink implements DynamicTableSink, SupportsWritingMetadata {

    private final DataType physicalDataType;
    private final EncodingFormat<SerializationSchema<RowData>> encodingFormat;
    private final QueueDestination queue;
    private final TargetSpec target;
    private final boolean addressMetadataNotNull;
    private final CloudTasksWriterOptions writerOptions;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final Integer parallelism;

    private List<String> metadataKeys = Collections.emptyList();

    public CloudTasksDynamicSink(
            DataType physicalDataType,
            EncodingFormat<SerializationSchema<RowData>> encodingFormat,
            QueueDestination queue,
            TargetSpec target,
            boolean addressMetadataNotNull,
            CloudTasksWriterOptions writerOptions,
            @Nullable String serviceAccountKeyFile,
            @Nullable String emulatorEndpoint,
            @Nullable Integer parallelism) {
        this.physicalDataType =
                Preconditions.checkNotNull(physicalDataType, "physicalDataType must not be null");
        this.encodingFormat =
                Preconditions.checkNotNull(encodingFormat, "encodingFormat must not be null");
        this.queue = Preconditions.checkNotNull(queue, "queue must not be null");
        this.target = Preconditions.checkNotNull(target, "target must not be null");
        this.addressMetadataNotNull = addressMetadataNotNull;
        this.writerOptions =
                Preconditions.checkNotNull(writerOptions, "writerOptions must not be null");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
        this.parallelism = parallelism;
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return target.writableMetadata();
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        for (String key : metadataKeys) {
            WritableMetadata.of(key);
            if (!target.writableMetadata().containsKey(key)) {
                throw new ValidationException(
                        "Writable metadata '"
                                + key
                                + "' does not belong to the selected Cloud Tasks target family.");
            }
        }
        if (target.fixedAddress() == null) {
            int addressPosition = metadataKeys.indexOf(target.addressMetadata().getKey());
            if (addressPosition < 0) {
                throw missingAddress();
            }
            if (!addressMetadataNotNull) {
                throw new ValidationException(
                        "A table without '"
                                + target.addressOptionKey()
                                + "' must declare its writable '"
                                + target.addressMetadata().getKey()
                                + "' metadata"
                                + " column as STRING NOT NULL, so no row can silently lose its"
                                + " target.");
            }
        }
        this.metadataKeys = Collections.unmodifiableList(new ArrayList<>(metadataKeys));
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        if (target.fixedAddress() == null
                && !metadataKeys.contains(target.addressMetadata().getKey())) {
            throw missingAddress();
        }
        WritableMetadata[] selected =
                metadataKeys.stream().map(WritableMetadata::of).toArray(WritableMetadata[]::new);
        SerializationSchema<RowData> encoder =
                encodingFormat.createRuntimeEncoder(context, physicalDataType);
        RowDataSerializationSchema serializer =
                new RowDataSerializationSchema(
                        encoder, DataType.getFieldCount(physicalDataType), selected, target);

        CloudTasksSinkBuilder<RowData> builder =
                CloudTasksSink.<RowData>builder()
                        .queue(queue)
                        .serializer(serializer)
                        .writerOptions(writerOptions);
        int taskIdPosition = metadataKeys.indexOf(WritableMetadata.TASK_ID.getKey());
        if (taskIdPosition >= 0) {
            int rowIndex = DataType.getFieldCount(physicalDataType) + taskIdPosition;
            builder.taskIdExtractor(new MetadataColumnTaskId(rowIndex));
        }
        if (serviceAccountKeyFile != null) {
            builder.serviceAccountKeyFile(serviceAccountKeyFile);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        Sink<RowData> sink = builder.build();
        return SinkV2Provider.of(sink, parallelism);
    }

    /**
     * Reads the deduplication key from the row's {@code task-id} metadata column.
     *
     * <p>A named type rather than a lambda because it travels in the job graph inside the sink: a
     * lambda would be restored by its {@code SerializedLambda} synthetic-method name, which the
     * compiler picks and no connector release pins.
     */
    private static final class MetadataColumnTaskId implements TaskIdExtractor<RowData> {

        private static final long serialVersionUID = 1L;

        private final int rowIndex;

        MetadataColumnTaskId(int rowIndex) {
            this.rowIndex = rowIndex;
        }

        @Override
        public String extractTaskId(RowData row) {
            return row.isNullAt(rowIndex) ? null : row.getString(rowIndex).toString();
        }
    }

    private ValidationException missingAddress() {
        return new ValidationException(
                "A Cloud Tasks table requires either option '"
                        + target.addressOptionKey()
                        + "' or a writable '"
                        + target.addressMetadata().getKey()
                        + "' metadata column declared STRING NOT NULL.");
    }

    @Override
    public DynamicTableSink copy() {
        CloudTasksDynamicSink copy =
                new CloudTasksDynamicSink(
                        physicalDataType,
                        encodingFormat,
                        queue,
                        target,
                        addressMetadataNotNull,
                        writerOptions,
                        serviceAccountKeyFile,
                        emulatorEndpoint,
                        parallelism);
        copy.metadataKeys = metadataKeys;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Cloud Tasks table sink";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CloudTasksDynamicSink that = (CloudTasksDynamicSink) o;
        return physicalDataType.equals(that.physicalDataType)
                && encodingFormat.equals(that.encodingFormat)
                && queue.equals(that.queue)
                && target.equals(that.target)
                && addressMetadataNotNull == that.addressMetadataNotNull
                && writerOptions.equals(that.writerOptions)
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
                queue,
                target,
                addressMetadataNotNull,
                writerOptions,
                serviceAccountKeyFile,
                emulatorEndpoint,
                parallelism,
                metadataKeys);
    }
}
