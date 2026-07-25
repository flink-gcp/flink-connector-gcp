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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Immutable sink configuration assembled by {@link CloudTasksSinkBuilder}.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public final class CloudTasksSinkConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DestinationResolver<? super T> destinationResolver;
    private final CloudTasksSerializationSchema<? super T> serializer;
    @Nullable private final TaskIdExtractor<? super T> taskIdExtractor;
    private final CloudTasksWriterOptions writerOptions;
    @Nullable private final String emulatorEndpoint;

    CloudTasksSinkConfig(
            DestinationResolver<? super T> destinationResolver,
            CloudTasksSerializationSchema<? super T> serializer,
            @Nullable TaskIdExtractor<? super T> taskIdExtractor,
            CloudTasksWriterOptions writerOptions,
            @Nullable String emulatorEndpoint) {
        this.destinationResolver = destinationResolver;
        this.serializer = serializer;
        this.taskIdExtractor = taskIdExtractor;
        this.writerOptions = writerOptions;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /** Returns the per-record destination resolver. */
    public DestinationResolver<? super T> getDestinationResolver() {
        return destinationResolver;
    }

    /** Returns the record serialization schema. */
    public CloudTasksSerializationSchema<? super T> getSerializer() {
        return serializer;
    }

    /**
     * Returns the deduplication-key extractor, or {@code null} when the sink creates unnamed tasks.
     */
    @Nullable
    public TaskIdExtractor<? super T> getTaskIdExtractor() {
        return taskIdExtractor;
    }

    /** Returns the writer tuning options. */
    public CloudTasksWriterOptions getWriterOptions() {
        return writerOptions;
    }

    /**
     * Returns the emulator endpoint ({@code host:port}), or {@code null} for production Cloud
     * Tasks.
     */
    @Nullable
    public String getEmulatorEndpoint() {
        return emulatorEndpoint;
    }
}
