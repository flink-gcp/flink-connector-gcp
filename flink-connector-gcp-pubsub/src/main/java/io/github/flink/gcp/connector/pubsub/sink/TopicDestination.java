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

import org.apache.flink.annotation.Public;

import com.google.pubsub.v1.TopicName;
import io.github.flink.gcp.connector.base.options.ResourceNames;

import java.io.Serializable;
import java.util.Objects;

/**
 * A fully-qualified Pub/Sub topic reference: project and topic.
 *
 * <p>Instances are pure topic <em>identity</em>: {@link #equals(Object)} and {@link #hashCode()}
 * are defined over exactly (project, topic) so the class can serve as a per-destination key (the
 * writer's per-topic publisher map). Publisher settings are intentionally not part of this class —
 * they are configured on the sink — keeping destination identity stable.
 *
 * <p>Instances are immutable; the resource path and hash are precomputed, so they are cheap to use
 * as map keys on the per-record write path. Resolvers should still cache and reuse instances
 * instead of re-creating them per record.
 */
@Public
public final class TopicDestination implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String project;
    private final String topic;
    private final String topicPath;
    private final int hash;

    private TopicDestination(String project, String topic) {
        this.project = project;
        this.topic = topic;
        this.topicPath = TopicName.format(project, topic);
        this.hash = Objects.hash(project, topic);
    }

    /**
     * Creates a {@link TopicDestination} from bare ids, not resource paths.
     *
     * @param project the Google Cloud project id
     * @param topic the Pub/Sub topic id
     * @return the destination
     * @throws IllegalArgumentException if a component is null or blank, has leading or trailing
     *     whitespace, or contains {@code '/'} — a separator would make the composed resource path
     *     address a different resource
     */
    public static TopicDestination of(String project, String topic) {
        ResourceNames.checkComponent(project, "project");
        ResourceNames.checkComponent(topic, "topic");
        return new TopicDestination(project, topic);
    }

    /** Returns the Google Cloud project id, given as a bare id rather than a resource path. */
    public String getProject() {
        return project;
    }

    /** Returns the Pub/Sub topic id, given as a bare id rather than a resource path. */
    public String getTopic() {
        return topic;
    }

    /**
     * Returns the topic path in the {@code projects/<p>/topics/<t>} form used by the Pub/Sub API.
     */
    public String toTopicPath() {
        return topicPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicDestination that = (TopicDestination) o;
        return project.equals(that.project) && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return project + "/" + topic;
    }
}
