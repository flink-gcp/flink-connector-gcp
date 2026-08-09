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

package io.github.flink.gcp.connector.spanner.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.serializer.SpannerMutationSerializationSchema;

import javax.annotation.Nullable;

/**
 * Builder for the Spanner sink; created through {@link SpannerSink#builder()}.
 *
 * <p>{@link #database(SpannerDatabase)} and {@link #serializer} are required; everything else is
 * defaulted.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public class SpannerSinkBuilder<T> {

    @Nullable private SpannerDatabase database;
    @Nullable private SpannerMutationSerializationSchema<? super T> serializer;
    private SpannerWriterOptions writerOptions = SpannerWriterOptions.defaults();
    private FailureHandler<? super FailedMutation> failedMutationHandler = FailureHandler.failJob();
    private ConstraintViolationPolicy constraintViolationPolicy =
            ConstraintViolationPolicy.FAIL_JOB;
    @Nullable private EmulatorEndpoint emulatorEndpoint;

    SpannerSinkBuilder() {}

    /**
     * Sets the database to write to. Required.
     *
     * @param database the database
     * @return this builder
     */
    public SpannerSinkBuilder<T> database(SpannerDatabase database) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        return this;
    }

    /**
     * Sets the schema turning records into mutations. Required.
     *
     * @param serializer the serialization schema
     * @return this builder
     */
    public SpannerSinkBuilder<T> serializer(
            SpannerMutationSerializationSchema<? super T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Sets the writer's tuning options. Optional; defaults to {@link
     * SpannerWriterOptions#defaults()}.
     *
     * @param writerOptions the writer options
     * @return this builder
     */
    public SpannerSinkBuilder<T> writerOptions(SpannerWriterOptions writerOptions) {
        this.writerOptions =
                Preconditions.checkNotNull(writerOptions, "writerOptions must not be null");
        return this;
    }

    /**
     * Sets what happens to a mutation the service terminally refused. Optional; defaults to {@code
     * FailureHandler.failJob()}.
     *
     * @param failedMutationHandler the failure handler
     * @return this builder
     */
    public SpannerSinkBuilder<T> failedMutationHandler(
            FailureHandler<? super FailedMutation> failedMutationHandler) {
        this.failedMutationHandler =
                Preconditions.checkNotNull(
                        failedMutationHandler, "failedMutationHandler must not be null");
        return this;
    }

    /**
     * Sets what happens to a mutation Spanner refuses for violating a constraint — a {@code NULL}
     * in a {@code NOT NULL} column, an over-long value, a {@code CHECK} or foreign-key constraint.
     * Optional; defaults to {@link ConstraintViolationPolicy#FAIL_JOB}.
     *
     * <p>Under {@link ConstraintViolationPolicy#ROUTE_TO_FAILURE_HANDLER} such a mutation reaches
     * {@link #failedMutationHandler}, so that handler decides whether it fails the job, is dropped
     * or is dead-lettered.
     *
     * @param constraintViolationPolicy the policy
     * @return this builder
     */
    public SpannerSinkBuilder<T> constraintViolationPolicy(
            ConstraintViolationPolicy constraintViolationPolicy) {
        this.constraintViolationPolicy =
                Preconditions.checkNotNull(
                        constraintViolationPolicy, "constraintViolationPolicy must not be null");
        return this;
    }

    /**
     * Points the sink at a Spanner emulator instead of the real service. Optional; for tests.
     *
     * <p>The emulator needs no credentials, so setting this also stops the client from looking for
     * any.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public SpannerSinkBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint);
        return this;
    }

    /**
     * Builds the sink.
     *
     * @return the sink
     * @throws IllegalStateException if a required option is missing
     */
    public Sink<T> build() {
        Preconditions.checkState(
                database != null, "A database is required. Set it with database(...).");
        Preconditions.checkState(
                serializer != null, "A serializer is required. Set it with serializer(...).");
        return new SpannerMutationsSink<>(
                new SpannerSinkConfig<>(
                        database,
                        serializer,
                        writerOptions,
                        failedMutationHandler,
                        constraintViolationPolicy,
                        emulatorEndpoint));
    }
}
