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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.sink.topics.TopicAdmin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An in-memory {@link TopicAdmin} recording created topics, with a scriptable failure. */
final class FakeTopicAdmin implements TopicAdmin {

    final List<TopicDestination> created = new ArrayList<>();
    IOException createFailure;
    int closeCalls;

    @Override
    public void createTopic(TopicDestination destination) throws IOException {
        if (createFailure != null) {
            throw createFailure;
        }
        created.add(destination);
    }

    @Override
    public void close() {
        closeCalls++;
    }
}
