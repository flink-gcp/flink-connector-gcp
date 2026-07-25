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

package io.github.flink.gcp.connector.cloudtasks.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;

/**
 * The first stage of {@link CloudTasksSerializationSchema#httpTarget(String)}: it carries the
 * target URL and waits for the body schema.
 *
 * <p>This stage is not generic on purpose. {@link #withBody(SerializationSchema)} is what binds the
 * record type, so every method after it — {@code withUrl}, {@code withHeaders} — infers the record
 * type from the body schema and needs no explicit type witness.
 */
@PublicEvolving
public final class HttpTargetBuilder {

    private final String url;

    HttpTargetBuilder(String url) {
        this.url = HttpTargetSerializationSchema.checkUrl(url, "url");
    }

    /**
     * Sets the schema serializing a record into the task's HTTP body, and binds the record type of
     * the chain.
     *
     * <p>The body is sent under {@code POST} (the default), {@code PUT} and {@code PATCH}, the only
     * methods Cloud Tasks accepts one on; under any other method the schema is left unused and the
     * task carries no body.
     *
     * @param body the body schema
     * @param <T> type of the records written by the sink
     * @return the serialization schema, ready to use or to layer further options onto
     */
    public <T> HttpTargetSerializationSchema<T> withBody(SerializationSchema<T> body) {
        return HttpTargetSerializationSchema.of(url, body);
    }
}
