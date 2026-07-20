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

package io.github.flink.gcp.connector.pubsub.sink.topics;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;

import java.io.IOException;

/**
 * Topic administration operations used by the sink, abstracting the Pub/Sub admin client so writer
 * logic can be unit-tested without one.
 *
 * <p>Instances are created on the task manager inside {@code createWriter} and are never shipped in
 * the job graph, so the interface is not {@link java.io.Serializable}. It is {@link AutoCloseable}
 * so implementations holding a gRPC client (for example one injected for tests) can shut down its
 * channel and threads with the writer.
 */
@Internal
public interface TopicAdmin extends AutoCloseable {

    /**
     * Creates the given topic with default topic settings. Idempotent: creating a topic that
     * already exists (for example because a parallel subtask won the creation race) succeeds
     * silently.
     *
     * @param destination the topic to create
     * @throws IOException if the creation fails for any reason other than the topic already
     *     existing
     */
    void createTopic(TopicDestination destination) throws IOException;

    @Override
    void close() throws Exception;
}
