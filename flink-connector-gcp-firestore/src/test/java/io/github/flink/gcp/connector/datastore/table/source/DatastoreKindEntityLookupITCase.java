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

package io.github.flink.gcp.connector.datastore.table.source;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.StringValue;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupResponse;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Looks up through the lookup's own generated client against the emulator, mask included. */
@Testcontainers
class DatastoreKindEntityLookupITCase extends AbstractDatastoreEmulatorITCase {

    private static LookupResponse lookUp(DatastoreKindEntityLookup lookup, Key key)
            throws Exception {
        return lookup.lookupAsync(key).get(30, TimeUnit.SECONDS);
    }

    private static DatastoreLookupKeys keys(String kind) {
        return new DatastoreLookupKeys(database(), "", kind, false);
    }

    private static Key named(DatastoreLookupKeys keys, String name) {
        return keys.key(GenericRowData.of(StringData.fromString(name)));
    }

    @Test
    void aLookupAsksForEachPropertyLiterallyAndNoOther() throws Exception {
        String kind = uniqueKind();
        String unindexed = "u".repeat(2000);
        client().put(
                        Entity.newBuilder(key(kind, "d"))
                                .set("a.b", "literal")
                                .set(
                                        "a",
                                        EntityValue.of(
                                                FullEntity.newBuilder().set("b", "nested").build()))
                                .set("back`quote", "q")
                                .set("back\\slash", "s")
                                .set(
                                        "long",
                                        StringValue.newBuilder(unindexed)
                                                .setExcludeFromIndexes(true)
                                                .build())
                                .set("tags", ListValue.of("x", "y"))
                                .set("other", "unread")
                                .build());
        DatastoreLookupKeys keys = keys(kind);
        DatastoreKindEntityLookup lookup =
                new DatastoreKindEntityLookup(
                        database(),
                        new String[] {"a.b", "back`quote", "back\\slash", "long", "tags"},
                        emulatorEndpoint(),
                        null);
        lookup.open();
        try {
            LookupResponse answer = lookUp(lookup, named(keys, "d"));

            assertThat(answer.getFoundCount()).isEqualTo(1);
            Entity entity = Entity.fromPb(answer.getFound(0).getEntity());
            assertThat(entity.getNames())
                    .containsExactlyInAnyOrder("a.b", "back`quote", "back\\slash", "long", "tags");
            assertThat(entity.getString("a.b")).isEqualTo("literal");
            // An unindexed value and an array come back whole, unlike a projection query's.
            assertThat(entity.getString("long")).isEqualTo(unindexed);
            assertThat(entity.<StringValue>getList("tags"))
                    .extracting(StringValue::get)
                    .containsExactly("x", "y");
            assertThat(answer.hasReadTime()).isTrue();
            assertThat(answer.getFound(0).getVersion()).isPositive();
        } finally {
            lookup.close();
        }
    }

    @Test
    void aLookupOfNoPropertyReturnsTheEntityWithoutProperties() throws Exception {
        String kind = uniqueKind();
        client().put(Entity.newBuilder(key(kind, "d")).set("x", 1L).build());
        DatastoreLookupKeys keys = keys(kind);
        DatastoreKindEntityLookup lookup =
                new DatastoreKindEntityLookup(database(), new String[0], emulatorEndpoint(), null);
        lookup.open();
        try {
            LookupResponse found = lookUp(lookup, named(keys, "d"));
            LookupResponse missing = lookUp(lookup, named(keys, "missing"));

            assertThat(found.getFoundCount()).isEqualTo(1);
            assertThat(found.getFound(0).getEntity().getPropertiesMap()).isEmpty();
            assertThat(found.getFound(0).getVersion()).isPositive();
            assertThat(found.getFound(0).hasCreateTime()).isTrue();
            assertThat(found.getFound(0).hasUpdateTime()).isTrue();
            assertThat(missing.getFoundCount()).isZero();
            assertThat(missing.getMissingCount()).isEqualTo(1);
        } finally {
            lookup.close();
        }
    }

    @Test
    void aNegativeIdInANamedDatabaseAndNamespaceIsFound() throws Exception {
        String kind = uniqueKind();
        com.google.cloud.datastore.Key stored =
                com.google.cloud.datastore.Key.newBuilder(PROJECT, kind, -5L)
                        .setDatabaseId("other")
                        .setNamespace("tenant")
                        .build();
        try (Datastore other =
                DatastoreOptions.newBuilder()
                        .setProjectId(PROJECT)
                        .setDatabaseId("other")
                        .setHost(emulatorEndpoint())
                        .setCredentials(NoCredentials.getInstance())
                        .build()
                        .getService()) {
            other.put(Entity.newBuilder(stored).set("x", 1L).build());
        }
        DatastoreLookupKeys keys =
                new DatastoreLookupKeys(
                        DatabaseDestination.of(PROJECT, "other"), "tenant", kind, true);
        DatastoreKindEntityLookup lookup =
                new DatastoreKindEntityLookup(
                        DatabaseDestination.of(PROJECT, "other"),
                        new String[] {"x"},
                        emulatorEndpoint(),
                        null);
        lookup.open();
        try {
            LookupResponse found = lookUp(lookup, keys.key(GenericRowData.of(-5L)));

            assertThat(found.getFoundCount()).isEqualTo(1);
            assertThat(found.getFound(0).getEntity().getKey().getPath(0).getId()).isEqualTo(-5L);
        } finally {
            lookup.close();
        }
        DatastoreLookupKeys defaultDatabase =
                new DatastoreLookupKeys(database(), "tenant", kind, true);
        DatastoreKindEntityLookup elsewhere =
                new DatastoreKindEntityLookup(
                        database(), new String[] {"x"}, emulatorEndpoint(), null);
        elsewhere.open();
        try {
            assertThat(
                            lookUp(elsewhere, defaultDatabase.key(GenericRowData.of(-5L)))
                                    .getMissingCount())
                    .as("the default database holds no such entity")
                    .isEqualTo(1);
        } finally {
            elsewhere.close();
        }
    }
}
