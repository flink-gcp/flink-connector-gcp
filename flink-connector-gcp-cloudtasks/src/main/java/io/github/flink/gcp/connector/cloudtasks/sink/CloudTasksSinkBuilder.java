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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.cloudtasks.sink.serializer.CloudTasksSerializationSchema;

import javax.annotation.Nullable;

/**
 * Builder for Cloud Tasks sinks, obtained from {@link CloudTasksSink#builder()}.
 *
 * <p>Required settings: a serialization schema and a destination. The destination is set through
 * either {@link #queue(QueueDestination)} (fixed queue) or {@link
 * #destinationResolver(DestinationResolver)} (per-record dynamic destinations); the two override
 * each other and the last call wins.
 *
 * <p>The queue itself is never created by the sink and must exist: an auto-created queue would
 * carry Cloud Tasks' default rate limits, silently discarding the pacing that is the reason to use
 * the service, and a deleted queue name cannot be reused for 3 days.
 *
 * @param <T> type of the records written by the sink
 */
@Public
public class CloudTasksSinkBuilder<T> {

    private DestinationResolver<? super T> destinationResolver;
    private CloudTasksSerializationSchema<? super T> serializer;
    @Nullable private TaskIdExtractor<? super T> taskIdExtractor;
    private CloudTasksWriterOptions writerOptions = CloudTasksWriterOptions.defaults();
    private FailureHandler<? super FailedTask> failedTaskHandler = FailureHandler.failJob();
    @Nullable private String serviceAccountKeyFile;
    @Nullable private EmulatorEndpoint emulatorEndpoint;

    CloudTasksSinkBuilder() {}

    /**
     * Creates every task in the given fixed queue. Overrides any previously set queue or resolver.
     *
     * @param queue the destination queue
     * @return this builder
     */
    public CloudTasksSinkBuilder<T> queue(QueueDestination queue) {
        this.destinationResolver =
                new FixedDestinationResolver(
                        Preconditions.checkNotNull(queue, "queue must not be null"));
        return this;
    }

    /**
     * Resolves the destination queue per record (dynamic destinations). Overrides any previously
     * set queue or resolver.
     *
     * @param destinationResolver the resolver
     * @return this builder
     */
    public CloudTasksSinkBuilder<T> destinationResolver(
            DestinationResolver<? super T> destinationResolver) {
        this.destinationResolver =
                Preconditions.checkNotNull(
                        destinationResolver, "destinationResolver must not be null");
        return this;
    }

    /**
     * Sets the record serialization schema.
     *
     * @param serializer the serialization schema
     * @return this builder
     */
    public CloudTasksSinkBuilder<T> serializer(
            CloudTasksSerializationSchema<? super T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Opts into named tasks, deduplicating records by the extracted key. Optional; without it the
     * sink creates unnamed tasks and a record replayed after a failure calls the endpoint twice.
     *
     * <p>The sink hashes the key with SHA-256 before using it as the task id, and a repeated create
     * for a key Cloud Tasks still remembers counts as success. Naming is off by default because
     * Google documents the duplicate-name lookup as significantly increasing create latency, and
     * because the window in which a key is remembered is bounded — its own documentation gives both
     * "up to 24 hours" and "~1 hour" for it, so design against the shorter one.
     *
     * @param taskIdExtractor the deduplication-key extractor
     * @return this builder
     */
    public CloudTasksSinkBuilder<T> taskIdExtractor(TaskIdExtractor<? super T> taskIdExtractor) {
        this.taskIdExtractor =
                Preconditions.checkNotNull(taskIdExtractor, "taskIdExtractor must not be null");
        return this;
    }

    /**
     * Sets the writer tuning options (the in-flight cap and the two retry budgets). Optional;
     * defaults to {@link CloudTasksWriterOptions#defaults()}.
     *
     * @param writerOptions the options
     * @return this builder
     */
    public CloudTasksSinkBuilder<T> writerOptions(CloudTasksWriterOptions writerOptions) {
        this.writerOptions =
                Preconditions.checkNotNull(writerOptions, "writerOptions must not be null");
        return this;
    }

    /**
     * Sets the policy for a task that terminally fails — fail the job (the default), drop it, or
     * send it to a dead-letter queue. Only data-shaped failures reach it: a record the serializer
     * rejects, a task id extractor that throws, and a creation the service rejects with {@code
     * INVALID_ARGUMENT}. Everything else keeps failing the job, including an exhausted retry budget
     * and {@code PERMISSION_DENIED} — see the connector documentation for the full routing table.
     *
     * <p>The parameter is contravariant, so a handler written against the shared {@code
     * FailedElement} contract serves every connector in this repository without a cast.
     *
     * @param failedTaskHandler the handler
     * @return this builder
     */
    public CloudTasksSinkBuilder<T> failedTaskHandler(
            FailureHandler<? super FailedTask> failedTaskHandler) {
        this.failedTaskHandler =
                Preconditions.checkNotNull(failedTaskHandler, "failedTaskHandler must not be null");
        return this;
    }

    /**
     * Authenticates the sink with the service-account JSON key at the given path instead of
     * application-default credentials. The file is read on each TaskManager when its writer is
     * created, so the same path must be readable by every TaskManager that can run this sink.
     * Optional; when unset the sink uses application-default credentials.
     *
     * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
     * Workload Identity where the deployment supports one. This setting cannot be combined with
     * {@link #emulatorEndpoint(String)}, whose plaintext channel deliberately carries no
     * credentials.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public CloudTasksSinkBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the sink at a Cloud Tasks emulator instead of the production service. The connection
     * to the given {@code host:port} uses a plaintext channel with no credentials, so this must
     * only ever be used against an emulator. Optional; when unset the sink connects to Cloud Tasks
     * with application-default credentials.
     *
     * <p>The value is parsed here, so a malformed {@code host:port} is rejected on the client
     * instead of surfacing as a connection failure once the job has been deployed.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public CloudTasksSinkBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
        return this;
    }

    /**
     * Builds the sink.
     *
     * @return the sink
     */
    public Sink<T> build() {
        Preconditions.checkState(serializer != null, "A serializer is required.");
        Preconditions.checkState(
                destinationResolver != null,
                "A destination is required: set queue(...) or destinationResolver(...).");
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        Preconditions.checkState(
                writerOptions.getChannelPoolSize() == null || emulatorEndpoint == null,
                "channelPoolSize(...) cannot be combined with emulatorEndpoint(...): an emulator"
                        + " always uses one plaintext channel, so the pool would be silently"
                        + " ignored. Remove one of the two settings.");
        return new CloudTasksCreateTaskSink<>(
                new CloudTasksSinkConfig<>(
                        destinationResolver,
                        serializer,
                        taskIdExtractor,
                        writerOptions,
                        failedTaskHandler,
                        serviceAccountKeyFile,
                        emulatorEndpoint));
    }
}
