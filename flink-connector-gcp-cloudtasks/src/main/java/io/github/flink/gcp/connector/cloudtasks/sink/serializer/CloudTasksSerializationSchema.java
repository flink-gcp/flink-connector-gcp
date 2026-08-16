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

package io.github.flink.gcp.connector.cloudtasks.sink.serializer;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.serialization.SerializationSchema;

import com.google.cloud.tasks.v2.Task;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;

/**
 * Serializes sink records into Cloud Tasks tasks.
 *
 * <p>Implementations return a full {@link Task}, so every per-record field of a task — URL, HTTP
 * method, headers, body, routing, schedule time, dispatch deadline, authorization — is expressible.
 * Two bounds worth knowing when using them: a schedule time may be at most 30 days ahead, and an
 * HTTP target's dispatch deadline must be between 15 seconds and 30 minutes.
 *
 * <p>The task <b>name</b> is the exception: it is composed by the sink from the resolved queue and
 * the SHA-256 digest of the key returned by {@code
 * CloudTasksSinkBuilder#taskIdExtractor(TaskIdExtractor)}, so an implementation must leave it
 * unset. The writer rejects a named task rather than passing the name through, which is what keeps
 * the hashing free of a second path around it.
 *
 * <p>Returning {@code null} skips the record: it is written nowhere and is not a failure. Every
 * serializer of this connector family reads {@code null} that way, so a filter that depends on the
 * task being built belongs here rather than upstream of the sink.
 *
 * <p>The common case — an HTTP target whose body is a serialized record — is covered by {@link
 * #httpTarget(String)}:
 * <!-- javadoc-example file="JavadocCloudTasksExamples.java" tag="http-target" -->
 *
 * <pre>{@code
 * CloudTasksSerializationSchema.httpTarget("https://api.example.com/v1/orders")
 *         .withBody(new MyEventJsonSerializationSchema())
 *         .withHeaders(e -> Map.of("Content-Type", "application/json"))
 *         .withOidcToken("dispatcher@my-project.iam.gserviceaccount.com");
 * }</pre>
 *
 * <p>An App Engine target uses a relative URI and optional service, version, and instance routing:
 * <!-- javadoc-example file="JavadocCloudTasksExamples.java" tag="app-engine-target" -->
 *
 * <pre>{@code
 * CloudTasksSerializationSchema.appEngineTarget("/tasks/orders")
 *         .withBody(new MyEventJsonSerializationSchema())
 *         .withRouting(AppEngineRouting.newBuilder().setService("worker").build())
 *         .build();
 * }</pre>
 *
 * @param <T> type of the records written by the sink
 */
@Public
public interface CloudTasksSerializationSchema<T> extends Serializable {

    /**
     * Initialization hook invoked once before serialization starts, on the task that runs the
     * writer. The default implementation does nothing.
     *
     * @param context contextual information for initialization (metrics, user code class loader)
     * @throws Exception if initialization fails; fails the writer creation
     */
    default void open(SerializationSchema.InitializationContext context) throws Exception {}

    /**
     * Serializes the given record into a Cloud Tasks task.
     *
     * @param element the record
     * @return the task to create, carrying no name, or {@code null} to skip the record
     * @throws IOException if the record cannot be serialized; the record is handed to the sink's
     *     failed-task handler, which fails the job by default
     */
    @Nullable
    Task serialize(T element) throws IOException;

    /**
     * Starts building a schema producing HTTP-target tasks for the given URL.
     *
     * <p>The endpoint must be reachable from Cloud Tasks — in practice one with an external IP
     * address, or a Cloud Run service on its default {@code run.app} URL, whose requests stay on
     * the Google network even with internal-only ingress.
     *
     * <p>The returned stage exists only to let {@link
     * HttpTargetBuilder#withBody(SerializationSchema)} bind the record type, so the rest of the
     * chain infers it without a type witness.
     *
     * @param url the target URL, an absolute {@code http://} or {@code https://} URL
     * @return the builder stage
     */
    static HttpTargetBuilder httpTarget(String url) {
        return new HttpTargetBuilder(url);
    }

    /**
     * Starts an App Engine target builder for the given relative URI.
     *
     * <p>The task is delivered to the App Engine application in the queue's project. The queue and
     * application must use corresponding regions. Task-level service, version, and instance routing
     * can be added after the body binds the record type; a queue-level {@code
     * appEngineRoutingOverride} takes precedence over it.
     *
     * <p>{@link AppEngineTargetBuilder#withBody(SerializationSchema)} binds the record type. The
     * builder then accepts optional request settings and produces an immutable schema through
     * {@link AppEngineTargetBuilder#build()}.
     *
     * @param relativeUri an empty string for the root path, or a path beginning with {@code /} and
     *     an optional query string
     * @return the App Engine target builder before its body type is bound
     */
    static AppEngineTargetBuilder<Void> appEngineTarget(String relativeUri) {
        return new AppEngineTargetBuilder<>(relativeUri);
    }
}
