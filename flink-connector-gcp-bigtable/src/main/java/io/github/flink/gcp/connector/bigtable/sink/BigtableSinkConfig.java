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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Immutable sink configuration assembled by {@link BigtableSinkBuilder}.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public final class BigtableSinkConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DestinationResolver<? super T> destinationResolver;
    private final BigtableSerializationSchema<? super T> serializer;
    @Nullable private final String appProfileId;
    private final BigtableWriterOptions writerOptions;
    private final FailureHandler<? super FailedMutation> failedMutationHandler;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    private final CreateDisposition createDisposition;
    @Nullable private final TableCreateOptions tableCreateOptions;

    BigtableSinkConfig(
            DestinationResolver<? super T> destinationResolver,
            BigtableSerializationSchema<? super T> serializer,
            @Nullable String appProfileId,
            BigtableWriterOptions writerOptions,
            FailureHandler<? super FailedMutation> failedMutationHandler,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            CreateDisposition createDisposition,
            @Nullable TableCreateOptions tableCreateOptions) {
        this.destinationResolver = destinationResolver;
        this.serializer = serializer;
        this.appProfileId = appProfileId;
        this.writerOptions = writerOptions;
        this.failedMutationHandler = failedMutationHandler;
        this.emulatorEndpoint = emulatorEndpoint;
        this.createDisposition = createDisposition;
        this.tableCreateOptions = tableCreateOptions;
    }

    /** Returns the resolver naming the table each record is written to. */
    public DestinationResolver<? super T> getDestinationResolver() {
        return destinationResolver;
    }

    /** Returns the record serialization schema. */
    public BigtableSerializationSchema<? super T> getSerializer() {
        return serializer;
    }

    /**
     * Returns the application profile the client routes through, or {@code null} for the instance's
     * default profile.
     */
    @Nullable
    public String getAppProfileId() {
        return appProfileId;
    }

    /** Returns the writer tuning options. */
    public BigtableWriterOptions getWriterOptions() {
        return writerOptions;
    }

    /** Returns the policy for mutations that terminally fail. */
    public FailureHandler<? super FailedMutation> getFailedMutationHandler() {
        return failedMutationHandler;
    }

    /** Returns the emulator endpoint, or {@code null} for production Bigtable. */
    @Nullable
    public EmulatorEndpoint getEmulatorEndpoint() {
        return emulatorEndpoint;
    }

    /** Returns whether the sink may create a missing table. */
    public CreateDisposition getCreateDisposition() {
        return createDisposition;
    }

    /**
     * Returns the settings for the table the sink creates, or {@code null} under {@link
     * CreateDisposition#CREATE_NEVER} — the builder rejects every other combination.
     */
    @Nullable
    public TableCreateOptions getTableCreateOptions() {
        return tableCreateOptions;
    }
}
