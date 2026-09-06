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

package io.github.flink.gcp.connector.base.lineage.internal;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.lineage.ResourceIdentifier;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Canonical names from already parsed resource components. These factories perform no resource
 * discovery, service-name validation, case folding, or identifier parsing.
 */
@Internal
public final class LineageIdentifiers {

    private LineageIdentifiers() {}

    /** Names a configured BigQuery table or explicitly named view without discovering its type. */
    public static ResourceIdentifier bigQueryTable(String project, String dataset, String table) {
        return new ResourceIdentifier(
                "bigquery-table",
                "bigquery",
                project + "." + dataset + "." + table,
                Map.of("project", project, "dataset", dataset, "table", table));
    }

    /** Names a configured Pub/Sub topic. */
    public static ResourceIdentifier pubSubTopic(String project, String topic) {
        return new ResourceIdentifier(
                "pubsub-topic",
                "pubsub",
                "topic:" + project + ":" + topic,
                Map.of("project", project, "topic", topic));
    }

    /** Names a configured Pub/Sub subscription without looking up its topic. */
    public static ResourceIdentifier pubSubSubscription(String project, String subscription) {
        return new ResourceIdentifier(
                "pubsub-subscription",
                "pubsub",
                "subscription:" + project + ":" + subscription,
                Map.of("project", project, "subscription", subscription));
    }

    /** Names a configured Bigtable table. */
    public static ResourceIdentifier bigtableTable(String project, String instance, String table) {
        return new ResourceIdentifier(
                "bigtable-table",
                "bigtable://" + project + "/" + instance,
                table,
                Map.of("project", project, "instance", instance, "table", table));
    }

    /**
     * Names a Spanner table using the connector's existing identifier parser output, while
     * retaining the original configured schema and table syntax in the physical identity. A null or
     * empty parsed schema omits the schema segment in the canonical name. Pass null for an absent
     * configured schema; a non-null configured value is retained verbatim.
     */
    public static ResourceIdentifier spannerTable(
            String project,
            String instance,
            String database,
            @Nullable String schema,
            String table,
            @Nullable String configuredSchema,
            String configuredTable) {
        Map<String, String> identity = new HashMap<>();
        identity.put("project", project);
        identity.put("instance", instance);
        identity.put("database", database);
        identity.put("table", configuredTable);
        if (configuredSchema != null) {
            identity.put("schema", configuredSchema);
        }
        return new ResourceIdentifier(
                "spanner-table",
                "spanner://" + project + ":" + instance,
                database + "." + (schema == null || schema.isEmpty() ? "" : schema + ".") + table,
                identity);
    }

    /** Names a configured Change Stream, without discovering the tables it watches. */
    public static ResourceIdentifier spannerChangeStream(
            String project, String instance, String database, String stream) {
        return new ResourceIdentifier(
                "spanner-change-stream",
                "spanner://" + project + ":" + instance,
                database + "/changeStreams/" + stream,
                Map.of(
                        "project",
                        project,
                        "instance",
                        instance,
                        "database",
                        database,
                        "stream",
                        stream));
    }

    /** Names a configured Cloud Tasks queue, without reporting task URLs or request payloads. */
    public static ResourceIdentifier cloudTasksQueue(
            String project, String location, String queue) {
        return new ResourceIdentifier(
                "cloudtasks-queue",
                "cloudtasks://" + project + "/" + location,
                queue,
                Map.of("project", project, "location", location, "queue", queue));
    }
}
