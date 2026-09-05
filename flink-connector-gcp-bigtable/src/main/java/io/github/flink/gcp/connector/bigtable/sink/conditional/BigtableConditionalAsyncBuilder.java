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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.DestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.FixedDestinationResolver;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;

/**
 * Builder for {@link BigtableConditionalAsync}.
 *
 * @param <T> the input type
 */
@PublicEvolving
public final class BigtableConditionalAsyncBuilder<T> {
    private DestinationResolver<? super T> destinationResolver;
    private ConditionalSerializationSchema<? super T> serializer;
    private String appProfileId;
    private BigtableRequestOptions requestOptions = BigtableRequestOptions.builder().build();
    private EmptyBranchPolicy emptyBranchPolicy = EmptyBranchPolicy.IGNORE;
    private String serviceAccountKeyFile;
    private EmulatorEndpoint emulatorEndpoint;

    BigtableConditionalAsyncBuilder() {}

    /**
     * Writes to one table; this and destinationResolver are last-writer-wins.
     *
     * @param table the table
     * @return this builder
     */
    public BigtableConditionalAsyncBuilder<T> table(TableDestination table) {
        this.destinationResolver =
                new FixedDestinationResolver(
                        Preconditions.checkNotNull(table, "table must not be null"));
        return this;
    }

    /**
     * Resolves each destination before serialization. The async surface supplies a null writer
     * context.
     *
     * @param destinationResolver the resolver
     * @return this builder
     */
    public BigtableConditionalAsyncBuilder<T> destinationResolver(
            DestinationResolver<? super T> destinationResolver) {
        this.destinationResolver =
                Preconditions.checkNotNull(
                        destinationResolver, "destinationResolver must not be null");
        return this;
    }

    /**
     * Sets the required schema; a null serialization result skips an input.
     *
     * @param serializer the schema
     * @return this builder
     */
    public BigtableConditionalAsyncBuilder<T> serializer(
            ConditionalSerializationSchema<? super T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Selects an application profile; conditional writes require single-cluster routing and
     * single-row transactions.
     *
     * @param appProfileId the profile ID
     * @return this builder
     */
    public BigtableConditionalAsyncBuilder<T> appProfileId(String appProfileId) {
        Preconditions.checkNotNull(appProfileId, "appProfileId must not be null");
        Preconditions.checkArgument(!appProfileId.isBlank(), "appProfileId must not be blank");
        this.appProfileId = appProfileId;
        return this;
    }

    /**
     * Sets deadlines and bounded request/client capacity.
     *
     * @param requestOptions the options
     * @return this builder
     */
    public BigtableConditionalAsyncBuilder<T> requestOptions(
            BigtableRequestOptions requestOptions) {
        this.requestOptions =
                Preconditions.checkNotNull(requestOptions, "requestOptions must not be null");
        return this;
    }

    /**
     * Sets the successful empty-branch policy; FAIL can keep failing on replay after an applied
     * insertion.
     *
     * @param emptyBranchPolicy the policy
     * @return this builder
     */
    public BigtableConditionalAsyncBuilder<T> emptyBranchPolicy(
            EmptyBranchPolicy emptyBranchPolicy) {
        this.emptyBranchPolicy =
                Preconditions.checkNotNull(emptyBranchPolicy, "emptyBranchPolicy must not be null");
        return this;
    }

    /**
     * Selects a service-account key file.
     *
     * @param serviceAccountKeyFile the key-file path
     * @return this builder
     */
    public BigtableConditionalAsyncBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        Preconditions.checkNotNull(serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(
                !serviceAccountKeyFile.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        return this;
    }

    /**
     * Routes requests to an emulator.
     *
     * @param emulatorEndpoint the endpoint
     * @return this builder
     */
    public BigtableConditionalAsyncBuilder<T> emulatorEndpoint(EmulatorEndpoint emulatorEndpoint) {
        this.emulatorEndpoint =
                Preconditions.checkNotNull(emulatorEndpoint, "emulatorEndpoint must not be null");
        return this;
    }

    /**
     * Builds the configured async.
     *
     * @return the async
     */
    public BigtableConditionalAsync<T> build() {
        return new BigtableConditionalAsync<>(
                new ConditionalConfig<>(
                        destinationResolver,
                        serializer,
                        appProfileId,
                        requestOptions,
                        emptyBranchPolicy,
                        serviceAccountKeyFile,
                        emulatorEndpoint));
    }
}
