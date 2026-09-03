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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.FailedRequest;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Immutable configuration of a single-row request sink: what the per-operation sink builders
 * assemble and the runtime reads.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public final class SingleRowRequestConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DestinationResolver<? super T> destinationResolver;
    private final RowRequestSerializer<? super T> serializer;
    @Nullable private final String appProfileId;
    private final BigtableRequestOptions requestOptions;
    private final FailureHandler<? super FailedRequest> failedRequestHandler;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates the configuration.
     *
     * @param destinationResolver names the table each record is sent to
     * @param serializer builds each record's request
     * @param appProfileId the app profile, or {@code null} for the instance's default
     * @param requestOptions the runtime options
     * @param failedRequestHandler receives the requests that fail at the row level
     * @param serviceAccountKeyFile the service-account key file, or {@code null} for
     *     application-default credentials
     * @param emulatorEndpoint the emulator endpoint, or {@code null} for production Bigtable
     */
    public SingleRowRequestConfig(
            DestinationResolver<? super T> destinationResolver,
            RowRequestSerializer<? super T> serializer,
            @Nullable String appProfileId,
            BigtableRequestOptions requestOptions,
            FailureHandler<? super FailedRequest> failedRequestHandler,
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.destinationResolver =
                Preconditions.checkNotNull(
                        destinationResolver, "destinationResolver must not be null");
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        this.appProfileId = appProfileId;
        this.requestOptions =
                Preconditions.checkNotNull(requestOptions, "requestOptions must not be null");
        this.failedRequestHandler =
                Preconditions.checkNotNull(
                        failedRequestHandler, "failedRequestHandler must not be null");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /** Returns the resolver naming the table each record is sent to. */
    public DestinationResolver<? super T> getDestinationResolver() {
        return destinationResolver;
    }

    /** Returns the serializer building each record's request. */
    public RowRequestSerializer<? super T> getSerializer() {
        return serializer;
    }

    /** Returns the app profile, or {@code null} for the instance's default. */
    @Nullable
    public String getAppProfileId() {
        return appProfileId;
    }

    /** Returns the runtime options. */
    public BigtableRequestOptions getRequestOptions() {
        return requestOptions;
    }

    /** Returns the handler receiving the requests that fail at the row level. */
    public FailureHandler<? super FailedRequest> getFailedRequestHandler() {
        return failedRequestHandler;
    }

    /**
     * Returns the service-account key file, or {@code null} for application-default credentials.
     */
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /** Returns the emulator endpoint, or {@code null} for production Bigtable. */
    @Nullable
    public EmulatorEndpoint getEmulatorEndpoint() {
        return emulatorEndpoint;
    }
}
