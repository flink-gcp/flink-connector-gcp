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

package io.github.flink.gcp.connector.datastore.sink.writer;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StringValue;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PropertyMask;
import com.google.datastore.v1.Value;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the estimator equal to the growth of the {@code CommitRequest} the writer sends, built here
 * by hand from the same key and properties, its entity's size first held equal to the client
 * library's {@code Entity.calculateSerializedSize}.
 */
class MutationSizeEstimatorTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "orders");
    private static final Key KEY = Key.newBuilder("p", "K", "k", "orders").build();

    @Test
    void eachOperationAddsWhatItsMutationAddsToTheRequest() {
        Entity small = Entity.newBuilder(KEY).set("v", 1L).build();
        com.google.datastore.v1.Entity smallProto =
                entityProto(Value.newBuilder().setIntegerValue(1L).build());
        String text = "x".repeat(200_000);
        Entity large =
                Entity.newBuilder(KEY)
                        .set("v", StringValue.newBuilder(text).setExcludeFromIndexes(true).build())
                        .build();
        com.google.datastore.v1.Entity largeProto =
                entityProto(
                        Value.newBuilder()
                                .setStringValue(text)
                                .setExcludeFromIndexes(true)
                                .build());
        assertThat(Entity.calculateSerializedSize(small)).isEqualTo(smallProto.getSerializedSize());
        assertThat(Entity.calculateSerializedSize(large)).isEqualTo(largeProto.getSerializedSize());

        assertSizeIsTheRequestsGrowth(
                DatastoreMutation.insert(small), Mutation.newBuilder().setInsert(smallProto));
        assertSizeIsTheRequestsGrowth(
                DatastoreMutation.update(small), Mutation.newBuilder().setUpdate(smallProto));
        assertSizeIsTheRequestsGrowth(
                DatastoreMutation.upsert(small), Mutation.newBuilder().setUpsert(smallProto));
        assertSizeIsTheRequestsGrowth(
                DatastoreMutation.upsert(large), Mutation.newBuilder().setUpsert(largeProto));
        assertSizeIsTheRequestsGrowth(
                DatastoreMutation.upsertMerge(small),
                Mutation.newBuilder()
                        .setUpsert(smallProto)
                        .setPropertyMask(PropertyMask.newBuilder().addPaths("`v`")));
        assertSizeIsTheRequestsGrowth(
                DatastoreMutation.delete(KEY),
                Mutation.newBuilder().setDelete(EntityProtos.key(KEY)));
    }

    @Test
    void theRequestHeaderIsWhatARequestWithoutMutationsWeighs() {
        assertThat(MutationSizeEstimator.requestHeaderSize(DATABASE))
                .isEqualTo(header(DATABASE).build().getSerializedSize());
        DatabaseDestination defaultDatabase = DatabaseDestination.of("p");
        assertThat(MutationSizeEstimator.requestHeaderSize(defaultDatabase))
                .isEqualTo(header(defaultDatabase).build().getSerializedSize());
    }

    private static void assertSizeIsTheRequestsGrowth(
            DatastoreMutation mutation, Mutation.Builder proto) {
        long without = header(DATABASE).build().getSerializedSize();
        long with = header(DATABASE).addMutations(proto).build().getSerializedSize();

        assertThat(EncodedMutation.of(mutation).getSize())
                .as(mutation.getOperation().name())
                .isEqualTo(with - without);
    }

    private static CommitRequest.Builder header(DatabaseDestination database) {
        return CommitRequest.newBuilder()
                .setProjectId(database.getProject())
                .setDatabaseId(database.getDatabaseId())
                .setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
    }

    private static com.google.datastore.v1.Entity entityProto(Value value) {
        return com.google.datastore.v1.Entity.newBuilder()
                .setKey(EntityProtos.key(KEY))
                .putProperties("v", value)
                .build();
    }
}
