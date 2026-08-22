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
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class BigtableChangeStreamMutationTest {

    private static final ByteString ROW_KEY = ByteString.copyFromUtf8("row");
    private static final BigtableChangeStreamMutation.RawValue RAW =
            new BigtableChangeStreamMutation.RawValue(ByteString.copyFromUtf8("raw"));
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

    @Test
    void everyEntrySubtypeReportsOneKindOfItsOwn() {
        List<BigtableChangeStreamMutation.Entry> oneOfEach =
                Arrays.asList(
                        new BigtableChangeStreamMutation.SetCellEntry(
                                "family", ByteString.copyFromUtf8("qualifier"), 11L, ROW_KEY),
                        new BigtableChangeStreamMutation.DeleteCellsEntry(
                                "family",
                                ByteString.copyFromUtf8("qualifier"),
                                new BigtableChangeStreamMutation.TimestampRange(
                                        BigtableChangeStreamMutation.TimestampBound.unbounded(),
                                        BigtableChangeStreamMutation.TimestampBound.unbounded())),
                        new BigtableChangeStreamMutation.DeleteFamilyEntry("family"),
                        new BigtableChangeStreamMutation.AddToCellEntry("family", RAW, RAW, RAW),
                        new BigtableChangeStreamMutation.MergeToCellEntry("family", RAW, RAW, RAW));

        // The visitor makes an added subtype a compile error at every handler, but nothing makes
        // it a compile error to leave `EntryKind` — the discriminator callers branch on — without
        // a constant for the new subtype, or to hand two subtypes the same one. Both directions
        // fail here: a subtype missing from the list above breaks the first assertion, a kind no
        // subtype returns breaks the second.
        assertThat(oneOfEach)
                .extracting(Object::getClass)
                .containsExactlyInAnyOrderElementsOf(
                        subtypesOf(BigtableChangeStreamMutation.Entry.class));
        assertThat(oneOfEach)
                .extracting(BigtableChangeStreamMutation.Entry::getKind)
                .containsExactlyInAnyOrder(BigtableChangeStreamMutation.EntryKind.values());
    }

    @Test
    void everyValueSubtypeReportsOneTypeOfItsOwn() {
        List<BigtableChangeStreamMutation.Value> oneOfEach =
                Arrays.asList(
                        RAW,
                        new BigtableChangeStreamMutation.RawTimestamp(11L),
                        new BigtableChangeStreamMutation.Int64Value(12L));

        // The same pairing as the entry kinds above, held for the same reason.
        assertThat(oneOfEach)
                .extracting(Object::getClass)
                .containsExactlyInAnyOrderElementsOf(
                        subtypesOf(BigtableChangeStreamMutation.Value.class));
        assertThat(oneOfEach)
                .extracting(BigtableChangeStreamMutation.Value::getType)
                .containsExactlyInAnyOrder(BigtableChangeStreamMutation.ValueType.values());
    }

    /**
     * Returns a hierarchy's complete subtype set. Both hierarchies have private constructors and
     * every subtype is nested in {@link BigtableChangeStreamMutation}, so nothing outside that file
     * can extend them and no classpath scan is needed to enumerate them.
     */
    private static List<Class<?>> subtypesOf(Class<?> base) {
        return Arrays.stream(BigtableChangeStreamMutation.class.getDeclaredClasses())
                .filter(base::isAssignableFrom)
                .filter(candidate -> !candidate.equals(base))
                .collect(Collectors.toList());
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
