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

package io.github.flink.gcp.connector.pubsub.sink;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TopicDestination}. */
class TopicDestinationTest {

    @Test
    void exposesComponentsAndTopicPath() {
        TopicDestination destination = TopicDestination.of("my-project", "my-topic");

        assertThat(destination.getProject()).isEqualTo("my-project");
        assertThat(destination.getTopic()).isEqualTo("my-topic");
        assertThat(destination.toTopicPath()).isEqualTo("projects/my-project/topics/my-topic");
        assertThat(destination).hasToString("my-project/my-topic");
    }

    @Test
    void equalsAndHashCodeAreDefinedOverProjectAndTopic() {
        TopicDestination destination = TopicDestination.of("my-project", "my-topic");

        assertThat(destination)
                .isEqualTo(TopicDestination.of("my-project", "my-topic"))
                .hasSameHashCodeAs(TopicDestination.of("my-project", "my-topic"));
        assertThat(destination).isNotEqualTo(TopicDestination.of("my-project", "other-topic"));
        assertThat(destination).isNotEqualTo(TopicDestination.of("other-project", "my-topic"));
    }

    @Test
    void rejectsBlankComponents() {
        assertThatThrownBy(() -> TopicDestination.of("", "my-topic"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("project");
        assertThatThrownBy(() -> TopicDestination.of("my-project", "  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("topic");
        assertThatThrownBy(() -> TopicDestination.of(null, "my-topic"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsUntrimmedComponents() {
        assertThatThrownBy(() -> TopicDestination.of(" my-project", "my-topic"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whitespace");
        assertThatThrownBy(() -> TopicDestination.of("my-project", "my-topic "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("whitespace");
    }

    @Test
    void rejectsComponentsContainingSlashes() {
        assertThatThrownBy(() -> TopicDestination.of("my/project", "my-topic"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'/'");
        assertThatThrownBy(() -> TopicDestination.of("my-project", "projects/p/topics/t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'/'");
    }
}
