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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.google.cloud.tasks.v2.QueueName;

import java.io.Serializable;
import java.util.Objects;

/**
 * A fully-qualified Cloud Tasks queue reference: project, location and queue.
 *
 * <p>The location is part of the identity because queues are regional and one project may hold
 * queues in several regions.
 *
 * <p>Instances are pure queue <em>identity</em>: {@link #equals(Object)} and {@link #hashCode()}
 * are defined over exactly (project, location, queue). Queue configuration — the rate limits and
 * the retry policy that pace dispatch — is not expressible here: it belongs to the queue itself and
 * is applied by whoever creates it, not by this sink.
 *
 * <p>Instances are immutable; the resource path and hash are precomputed, so they are cheap to use
 * on the per-record write path. Resolvers should still cache and reuse instances instead of
 * re-creating them per record.
 */
@Public
public final class QueueDestination implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String project;
    private final String location;
    private final String queue;
    private final String queuePath;
    private final int hash;

    private QueueDestination(String project, String location, String queue) {
        this.project = project;
        this.location = location;
        this.queue = queue;
        this.queuePath = QueueName.format(project, location, queue);
        this.hash = Objects.hash(project, location, queue);
    }

    /**
     * Creates a {@link QueueDestination}.
     *
     * @param project the Google Cloud project id
     * @param location the queue's location (for example {@code asia-northeast1})
     * @param queue the queue id
     * @return the destination
     */
    public static QueueDestination of(String project, String location, String queue) {
        checkComponent(project, "project");
        checkComponent(location, "location");
        checkComponent(queue, "queue");
        return new QueueDestination(project, location, queue);
    }

    private static void checkComponent(String value, String name) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(value), "%s must not be blank", name);
        Preconditions.checkArgument(
                value.equals(value.trim()),
                "%s must not have leading or trailing whitespace: '%s'",
                name,
                value);
        Preconditions.checkArgument(
                value.indexOf('/') < 0, "%s must not contain '/': '%s'", name, value);
    }

    /** Returns the Google Cloud project id. */
    public String getProject() {
        return project;
    }

    /** Returns the queue's location. */
    public String getLocation() {
        return location;
    }

    /** Returns the queue id. */
    public String getQueue() {
        return queue;
    }

    /**
     * Returns the queue path in the {@code projects/PROJECT/locations/LOCATION/queues/QUEUE} form
     * used by the Cloud Tasks API.
     */
    public String toQueuePath() {
        return queuePath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QueueDestination that = (QueueDestination) o;
        return project.equals(that.project)
                && location.equals(that.location)
                && queue.equals(that.queue);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return project + "/" + location + "/" + queue;
    }
}
