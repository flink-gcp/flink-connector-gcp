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

package io.github.flink.gcp.connector.firestore.source;

import com.google.api.gax.rpc.UnimplementedException;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.QueryPartition;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.TransactionOptions;
import io.github.flink.gcp.connector.firestore.AbstractFirestoreEmulatorITCase;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins what the emulator does with the calls the source makes, where that is not what the service
 * does. Each test here backs a row of the emulator-deviation table on the connector's docs page; a
 * failure after an emulator bump means the row is stale, not that the source broke.
 */
class FirestoreEmulatorReadDeviationITCase extends AbstractFirestoreEmulatorITCase {

    @Test
    void theEmulatorDoesNotPartitionAQuery() throws Exception {
        String group = uniqueCollection();
        client().document(group + "/a").set(Map.of("n", 1)).get(30, TimeUnit.SECONDS);

        assertThatThrownBy(
                        () ->
                                client().collectionGroup(group)
                                        .getPartitions(2)
                                        .get(30, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(UnimplementedException.class);
        // One partition is the client library's own answer, made without a call.
        List<QueryPartition> one =
                client().collectionGroup(group).getPartitions(1).get(30, TimeUnit.SECONDS);
        assertThat(one).hasSize(1);
        assertThat(one.get(0).getStartAt()).isNull();
        assertThat(one.get(0).getEndBefore()).isNull();
    }

    @Test
    void theEmulatorReadsAtATimeOlderThanTheServiceKeeps() throws Exception {
        String collection = uniqueCollection();
        Timestamp written =
                client().document(collection + "/a")
                        .set(Map.of("n", 1))
                        .get(30, TimeUnit.SECONDS)
                        .getUpdateTime();
        Timestamp twoHoursAgo =
                Timestamp.ofTimeSecondsAndNanos(Timestamp.now().getSeconds() - 2 * 3600, 0);

        // The service refuses a read time older than an hour without point-in-time recovery; the
        // emulator answers it, and answers it as of that time: the document is not there yet.
        assertThat(readAt(collection, twoHoursAgo).isEmpty()).isTrue();
        assertThat(readAt(collection, written).size()).isOne();
    }

    private static QuerySnapshot readAt(String collection, Timestamp readTime) throws Exception {
        return client().runTransaction(
                        transaction -> transaction.get(client().collection(collection)).get(),
                        TransactionOptions.createReadOnlyOptionsBuilder()
                                .setReadTime(readTime.toProto())
                                .build())
                .get(30, TimeUnit.SECONDS);
    }
}
