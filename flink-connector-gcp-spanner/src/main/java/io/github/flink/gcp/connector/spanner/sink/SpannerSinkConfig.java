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

package io.github.flink.gcp.connector.spanner.sink;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * The immutable configuration {@link SpannerSinkBuilder} builds and the sink carries to the task
 * managers.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public final class SpannerSinkConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DatabaseDestination database;
    private final SpannerMutationSerializationSchema<? super T> serializer;
    private final SpannerWriterOptions writerOptions;
    private final FailureHandler<? super FailedMutation> failedMutationHandler;
    private final ConstraintViolationPolicy constraintViolationPolicy;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates the configuration. The only caller is {@link SpannerSinkBuilder#build()}, which has
     * already rejected a null for every one of these.
     */
    SpannerSinkConfig(
            DatabaseDestination database,
            SpannerMutationSerializationSchema<? super T> serializer,
            SpannerWriterOptions writerOptions,
            FailureHandler<? super FailedMutation> failedMutationHandler,
            ConstraintViolationPolicy constraintViolationPolicy,
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.database = database;
        this.serializer = serializer;
        this.writerOptions = writerOptions;
        this.failedMutationHandler = failedMutationHandler;
        this.constraintViolationPolicy = constraintViolationPolicy;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /** Returns the database to write to. */
    public DatabaseDestination getDatabase() {
        return database;
    }

    /** Returns the serialization schema. */
    public SpannerMutationSerializationSchema<? super T> getSerializer() {
        return serializer;
    }

    /** Returns the writer tuning options. */
    public SpannerWriterOptions getWriterOptions() {
        return writerOptions;
    }

    /** Returns the policy applied to terminally failed mutations. */
    public FailureHandler<? super FailedMutation> getFailedMutationHandler() {
        return failedMutationHandler;
    }

    /** Returns what happens to a mutation refused for violating a constraint. */
    public ConstraintViolationPolicy getConstraintViolationPolicy() {
        return constraintViolationPolicy;
    }

    /**
     * Returns the service-account key-file path, or {@code null} when no override is configured.
     */
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /** Returns the emulator endpoint, or {@code null} when writing to the real service. */
    @Nullable
    public EmulatorEndpoint getEmulatorEndpoint() {
        return emulatorEndpoint;
    }
}
