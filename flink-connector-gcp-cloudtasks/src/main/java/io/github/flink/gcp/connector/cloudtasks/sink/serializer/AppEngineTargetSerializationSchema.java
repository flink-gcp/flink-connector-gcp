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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

import com.google.cloud.tasks.v2.Task;

import java.io.IOException;

/**
 * A {@link CloudTasksSerializationSchema} producing App Engine target tasks.
 *
 * <p>Built by {@link AppEngineTargetBuilder}. Instances are immutable and contain no configuration
 * methods; all optional request settings belong to the builder.
 * <!-- javadoc-example file="JavadocCloudTasksExamples.java" tag="detailed-app-engine-target" -->
 *
 * <pre>{@code
 * CloudTasksSerializationSchema.appEngineTarget("/tasks/orders")
 *         .withBody(new MyEventJsonSerializationSchema())
 *         .withMethod(HttpMethod.POST)
 *         .withRelativeUri(e -> "/tasks/orders/" + e.orderId())
 *         .withRouting(
 *                 AppEngineRouting.newBuilder().setService("worker").setVersion("v2").build())
 *         .build();
 * }</pre>
 *
 * <p>Tasks produced here never carry a name. Naming remains the sink's responsibility through
 * {@code CloudTasksSinkBuilder#taskIdExtractor(TaskIdExtractor)}.
 *
 * <p>This schema never skips a record. A wrapped Flink serializer returning {@code null} is a
 * serialization failure rather than the connector's skip convention.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public final class AppEngineTargetSerializationSchema<T>
        implements CloudTasksSerializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final SerializationSchema<T> body;
    private final AppEngineTargetTaskConverter<T> converter;

    AppEngineTargetSerializationSchema(
            SerializationSchema<T> body, AppEngineTargetTaskConverter<T> converter) {
        this.body = body;
        this.converter = converter;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        body.open(context);
    }

    @Override
    public Task serialize(T element) throws IOException {
        byte[] payload = null;
        if (converter.carriesBody()) {
            payload = body.serialize(element);
            if (payload == null) {
                throw new IOException(
                        "The body schema "
                                + body.getClass().getName()
                                + " returned null for a record. Flink's SerializationSchema"
                                + " contract has no null in it, so this is a serialization failure"
                                + " rather than a skip; implement CloudTasksSerializationSchema"
                                + " directly to skip a record.");
            }
        }
        return converter.convert(element, payload);
    }
}
