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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Blob;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.LatLng;
import com.google.cloud.datastore.ListValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.StringValue;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatastoreMutationTest {

    private static final Key KEY =
            Key.newBuilder("p", "Order", "o-1")
                    .addAncestor(com.google.cloud.datastore.PathElement.of("Customer", 7L))
                    .setNamespace("ns")
                    .build();

    @Test
    void eachFactoryNamesItsOperationAndCarriesItsEntityOrKey() {
        Entity entity = Entity.newBuilder(KEY).set("v", 1L).build();

        assertThat(DatastoreMutation.upsert(entity).getOperation())
                .isEqualTo(DatastoreMutation.Operation.UPSERT);
        assertThat(DatastoreMutation.insert(entity).getOperation())
                .isEqualTo(DatastoreMutation.Operation.INSERT);
        assertThat(DatastoreMutation.update(entity).getOperation())
                .isEqualTo(DatastoreMutation.Operation.UPDATE);
        assertThat(DatastoreMutation.upsert(entity).getEntity()).isSameAs(entity);
        assertThat(DatastoreMutation.upsert(entity).getKey()).isEqualTo(KEY);

        DatastoreMutation delete = DatastoreMutation.delete(KEY);
        assertThat(delete.getOperation()).isEqualTo(DatastoreMutation.Operation.DELETE);
        assertThat(delete.getEntity()).isNull();
        assertThat(delete.getKey()).isEqualTo(KEY);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void anEntityWithoutACompleteKeyIsRefused() {
        FullEntity<IncompleteKey> incomplete =
                FullEntity.newBuilder(IncompleteKey.newBuilder("p", "Order").build())
                        .set("v", 1L)
                        .build();
        FullEntity<Key> unkeyed = (FullEntity) FullEntity.newBuilder().set("v", 1L).build();

        assertThatThrownBy(() -> DatastoreMutation.upsert((FullEntity) incomplete))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("complete key");
        assertThatThrownBy(() -> DatastoreMutation.insert(unkeyed))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Allocate ids before the sink");
        assertThatThrownBy(() -> DatastoreMutation.update(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> DatastoreMutation.delete(null))
                .isInstanceOf(NullPointerException.class);
    }

    /**
     * The client library's entity and key types are what the mutation carries, and a mutation
     * travels as Java serialization — to a dead-letter queue, and with the sink if a serializer
     * holds one. Every typed value the library offers survives the round trip, an embedded entity
     * with an incomplete key among them; the raw protobuf value wrapper is not exercised.
     */
    @Test
    void everyValueTypeSurvivesJavaSerialization() throws Exception {
        Entity embedded =
                Entity.newBuilder(Key.newBuilder("p", "Inner", 1L).build()).set("x", 1L).build();
        FullEntity<Key> entity =
                Entity.newBuilder(KEY)
                        .set("string", "s")
                        .set(
                                "unindexed",
                                StringValue.newBuilder("u").setExcludeFromIndexes(true).build())
                        .set("long", 1L)
                        .set("double", 1.5)
                        .set("boolean", true)
                        .set("timestamp", Timestamp.ofTimeSecondsAndNanos(1, 2))
                        .set("blob", Blob.copyFrom(new byte[] {1, 2}))
                        .set("latLng", LatLng.of(1.0, 2.0))
                        .set("key", Key.newBuilder("p", "Other", "k").build())
                        .set("entity", embedded)
                        .set(
                                "partial",
                                FullEntity.newBuilder(IncompleteKey.newBuilder("p", "Part").build())
                                        .set("x", 2L)
                                        .build())
                        .set("list", ListValue.of(1L, 2L))
                        .set("null", NullValue.of())
                        .build();

        for (DatastoreMutation write :
                new DatastoreMutation[] {
                    DatastoreMutation.upsert(entity),
                    DatastoreMutation.insert(entity),
                    DatastoreMutation.update(entity),
                    DatastoreMutation.delete(KEY)
                }) {
            DatastoreMutation copy = InstantiationUtil.clone(write);
            assertThat(copy).isEqualTo(write).hasSameHashCodeAs(write);
            assertThat(copy.getKey().getNamespace()).isEqualTo("ns");
        }
    }

    @Test
    void equalityCoversTheOperationTheKeyAndTheEntity() {
        Entity one = Entity.newBuilder(KEY).set("v", 1L).build();
        Entity two = Entity.newBuilder(KEY).set("v", 2L).build();

        assertThat(DatastoreMutation.upsert(one)).isEqualTo(DatastoreMutation.upsert(one));
        assertThat(DatastoreMutation.upsert(one)).isNotEqualTo(DatastoreMutation.insert(one));
        assertThat(DatastoreMutation.upsert(one)).isNotEqualTo(DatastoreMutation.upsert(two));
        assertThat(DatastoreMutation.delete(KEY).toString()).contains("DELETE").contains("o-1");
    }
}
