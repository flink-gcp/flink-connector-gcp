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

package io.github.flink.gcp.connector.pubsub.sink.topics;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.pubsub.v1.Topic;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PubSubTopicAdmin}'s option-to-protobuf translation, driven directly so the
 * mapping is verified without a client or an emulator, mirroring {@code
 * PubSubSubscriptionAdminTest} — the emulator round trip is {@code
 * PubSubTopicAutoCreationITCase}'s.
 */
class PubSubTopicAdminTest {

    private static final TopicDestination DESTINATION = TopicDestination.of("project", "topic");

    @Test
    void configuredCredentialsReachTheAdminSettings() throws Exception {
        NoCredentialsProvider credentials = NoCredentialsProvider.create();

        assertThat(
                        new PubSubTopicAdmin(null, credentials)
                                .clientSettings()
                                .getCredentialsProvider())
                .isSameAs(credentials);
    }

    @Test
    void unsetKnobsLeaveTheirProtoFieldsAlone() {
        Topic topic = PubSubTopicAdmin.toTopic(DESTINATION, TopicCreateOptions.builder().build());

        assertThat(topic.getName()).isEqualTo("projects/project/topics/topic");
        assertThat(topic.hasMessageRetentionDuration()).isFalse();
        assertThat(topic.getKmsKeyName()).isEmpty();
        assertThat(topic.hasMessageStoragePolicy()).isFalse();
    }

    @Test
    void nullOptionsTranslateToABareTopic() {
        Topic topic = PubSubTopicAdmin.toTopic(DESTINATION, null);

        assertThat(topic)
                .isEqualTo(Topic.newBuilder().setName("projects/project/topics/topic").build());
    }

    @Test
    void translatesEveryConfiguredKnob() {
        Topic topic =
                PubSubTopicAdmin.toTopic(
                        DESTINATION,
                        TopicCreateOptions.builder()
                                .messageRetention(Duration.ofDays(7).plusNanos(500))
                                .kmsKeyName("projects/p/locations/l/keyRings/r/cryptoKeys/k")
                                .allowedPersistenceRegions(
                                        Arrays.asList("europe-west1", "europe-west4"))
                                .enforceInTransit(true)
                                .build());

        assertThat(topic.getName()).isEqualTo("projects/project/topics/topic");
        assertThat(topic.getMessageRetentionDuration().getSeconds())
                .isEqualTo(Duration.ofDays(7).getSeconds());
        assertThat(topic.getMessageRetentionDuration().getNanos()).isEqualTo(500);
        assertThat(topic.getKmsKeyName())
                .isEqualTo("projects/p/locations/l/keyRings/r/cryptoKeys/k");
        assertThat(topic.getMessageStoragePolicy().getAllowedPersistenceRegionsList())
                .containsExactly("europe-west1", "europe-west4");
        assertThat(topic.getMessageStoragePolicy().getEnforceInTransit()).isTrue();
    }

    @Test
    void regionsWithoutEnforcementTranslateToAStoragePolicyThatOnlyRestrictsStorage() {
        Topic topic =
                PubSubTopicAdmin.toTopic(
                        DESTINATION,
                        TopicCreateOptions.builder()
                                .allowedPersistenceRegions(Arrays.asList("us-central1"))
                                .build());

        assertThat(topic.getMessageStoragePolicy().getAllowedPersistenceRegionsList())
                .containsExactly("us-central1");
        assertThat(topic.getMessageStoragePolicy().getEnforceInTransit()).isFalse();
    }
}
