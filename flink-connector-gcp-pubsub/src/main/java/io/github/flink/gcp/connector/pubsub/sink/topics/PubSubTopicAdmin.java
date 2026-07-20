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

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Default {@link TopicAdmin} backed by the Pub/Sub {@link TopicAdminClient}.
 *
 * <p>The client is created lazily on the first use, so jobs whose destination topics all exist
 * never construct it (and never open its gRPC channel). Creation conflicts ({@code ALREADY_EXISTS},
 * the topic was created concurrently — for example by a parallel subtask) are treated as success.
 */
@Internal
public class PubSubTopicAdmin implements TopicAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubTopicAdmin.class);

    private TopicAdminClient client;

    /** Creates an admin using application-default credentials. */
    public PubSubTopicAdmin() {}

    /**
     * Creates an admin using the given client.
     *
     * @param client the Pub/Sub admin client
     */
    public PubSubTopicAdmin(TopicAdminClient client) {
        this.client = client;
    }

    @Override
    public void createTopic(TopicDestination destination) throws IOException {
        TopicName topicName = TopicName.of(destination.getProject(), destination.getTopic());
        try {
            client().createTopic(topicName);
            LOG.info("Created Pub/Sub topic {}", destination);
        } catch (AlreadyExistsException e) {
            LOG.info("Pub/Sub topic {} already exists, not creating it", destination);
        } catch (RuntimeException e) {
            throw new IOException("Failed to create Pub/Sub topic " + destination, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    private TopicAdminClient client() throws IOException {
        if (client == null) {
            try {
                client = TopicAdminClient.create();
            } catch (IOException | RuntimeException e) {
                throw new IOException("Failed to create the Pub/Sub admin client", e);
            }
        }
        return client;
    }
}
