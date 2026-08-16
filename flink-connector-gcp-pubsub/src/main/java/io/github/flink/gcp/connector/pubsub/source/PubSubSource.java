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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.annotation.Public;

/**
 * Entry point for building Pub/Sub sources.
 * <!-- javadoc-example file="JavadocPubSubExamples.java" tag="source" -->
 *
 * <pre>{@code
 * Source<String, ?, ?> source =
 *         PubSubSource.<String>builder()
 *                 .subscription(SubscriptionDestination.of("my-project", "my-subscription"))
 *                 .deserializationSchema(
 *                         PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
 *                 .build();
 * }</pre>
 */
@Public
public final class PubSubSource {

    private PubSubSource() {}

    /**
     * Returns a new builder.
     *
     * @param <T> type of the records produced by the source
     * @return the builder
     */
    public static <T> PubSubSourceBuilder<T> builder() {
        return new PubSubSourceBuilder<>();
    }
}
