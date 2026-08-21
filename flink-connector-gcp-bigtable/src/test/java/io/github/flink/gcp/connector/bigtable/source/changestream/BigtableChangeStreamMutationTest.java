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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BigtableChangeStreamMutationTest {

    private static final ByteString ROW_KEY = ByteString.copyFromUtf8("row");
    private static final BigtableChangeStreamMutation.MutationType TYPE =
            BigtableChangeStreamMutation.MutationType.USER;
    private static final String CLUSTER = "cluster";
    private static final Instant COMMIT = Instant.parse("2026-08-12T00:00:00.123456789Z");
    private static final int TIE_BREAKER = 7;
    private static final String TOKEN = "token";
    private static final Instant WATERMARK = Instant.parse("2026-08-11T23:59:00.987654321Z");
    private static final List<BigtableChangeStreamMutation.Entry> ENTRIES = entries("value");

    @Test
    void equalityCoversEveryValue() {
        BigtableChangeStreamMutation mutation = mutation();
        List<BigtableChangeStreamMutation> mutated =
                Arrays.asList(
                        new BigtableChangeStreamMutation(
                                ByteString.copyFromUtf8("other-row"),
                                TYPE,
                                CLUSTER,
                                COMMIT,
                                TIE_BREAKER,
                                TOKEN,
                                WATERMARK,
                                ENTRIES),
                        new BigtableChangeStreamMutation(
                                ROW_KEY,
                                BigtableChangeStreamMutation.MutationType.GARBAGE_COLLECTION,
                                CLUSTER,
                                COMMIT,
                                TIE_BREAKER,
                                TOKEN,
                                WATERMARK,
                                ENTRIES),
                        new BigtableChangeStreamMutation(
                                ROW_KEY,
                                TYPE,
                                "other-cluster",
                                COMMIT,
                                TIE_BREAKER,
                                TOKEN,
                                WATERMARK,
                                ENTRIES),
                        new BigtableChangeStreamMutation(
                                ROW_KEY,
                                TYPE,
                                CLUSTER,
                                Instant.EPOCH,
                                TIE_BREAKER,
                                TOKEN,
                                WATERMARK,
                                ENTRIES),
                        new BigtableChangeStreamMutation(
                                ROW_KEY, TYPE, CLUSTER, COMMIT, 8, TOKEN, WATERMARK, ENTRIES),
                        new BigtableChangeStreamMutation(
                                ROW_KEY,
                                TYPE,
                                CLUSTER,
                                COMMIT,
                                TIE_BREAKER,
                                "other-token",
                                WATERMARK,
                                ENTRIES),
                        new BigtableChangeStreamMutation(
                                ROW_KEY,
                                TYPE,
                                CLUSTER,
                                COMMIT,
                                TIE_BREAKER,
                                TOKEN,
                                Instant.EPOCH,
                                ENTRIES),
                        new BigtableChangeStreamMutation(
                                ROW_KEY,
                                TYPE,
                                CLUSTER,
                                COMMIT,
                                TIE_BREAKER,
                                TOKEN,
                                WATERMARK,
                                Collections.emptyList()),
                        // Same entry count, different cell value. Without this case an `equals`
                        // comparing `entries.size()` would pass, and the serializer round trips
                        // that now lean on `equals` would stop seeing a corrupted entry.
                        new BigtableChangeStreamMutation(
                                ROW_KEY,
                                TYPE,
                                CLUSTER,
                                COMMIT,
                                TIE_BREAKER,
                                TOKEN,
                                WATERMARK,
                                entries("other-value")));

        // One case per field, plus the entry-content case: an `equals` that dropped any one of the
        // eight fields would let its mutation compare equal here.
        assertThat(mutated).hasSize(9).doesNotContain(mutation);
        // And a `hashCode` that read fewer fields than `equals` would collide with the base.
        assertThat(mutated).extracting(Object::hashCode).doesNotContain(mutation.hashCode());

        assertThat(mutation).isEqualTo(mutation()).hasSameHashCodeAs(mutation());
        assertThat(mutation).isEqualTo(mutation).isNotEqualTo(null).isNotEqualTo("not a mutation");
    }

    private static BigtableChangeStreamMutation mutation() {
        return new BigtableChangeStreamMutation(
                ROW_KEY, TYPE, CLUSTER, COMMIT, TIE_BREAKER, TOKEN, WATERMARK, ENTRIES);
    }

    private static List<BigtableChangeStreamMutation.Entry> entries(String value) {
        return Collections.singletonList(
                new BigtableChangeStreamMutation.SetCellEntry(
                        "family",
                        ByteString.copyFromUtf8("qualifier"),
                        11L,
                        ByteString.copyFromUtf8(value)));
    }
}
