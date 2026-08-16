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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;

import java.io.IOException;
import java.io.Serializable;

/**
 * Creates {@link TopicPublisher} instances for the writer's per-topic publisher map.
 *
 * <p>Serializable because the factory is shipped in the job graph; implementations must create all
 * client state at {@link #create(TopicDestination)} time, not at construction time.
 */
@Internal
public interface PublisherFactory extends Serializable {

    /**
     * Creates a publisher for the given topic.
     *
     * @param destination the destination topic
     * @return the publisher
     * @throws IOException if the publisher cannot be created
     */
    TopicPublisher create(TopicDestination destination) throws IOException;
}
