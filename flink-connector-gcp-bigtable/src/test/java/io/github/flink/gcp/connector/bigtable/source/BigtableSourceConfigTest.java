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

package io.github.flink.gcp.connector.bigtable.source;

import com.google.cloud.bigtable.data.v2.models.Query;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link BigtableSourceConfig}, and for the invariant that keeps the client's {@code
 * Query} out of the source's own state.
 *
 * <p>Why this is a reflective test rather than a round-trip one. A {@code Query} <em>is</em>
 * serializable, and — measured against google-cloud-bigtable 2.80.0 and protobuf-java 4.33.6 on
 * 2026-08-09, in four shapes including a field followed by a later-sorting field — it round-trips
 * with its trailing data intact, because {@code ObjectInputStream}'s block-data framing ends the
 * read its {@code mergeFrom} would otherwise run past. So a serialization test would pass on a
 * design this project has nevertheless decided against, for three reasons that hold regardless:
 * checkpointed state must have a byte format this connector owns rather than one a client upgrade
 * can move; a {@code Query} cannot be read back, since its target-id accessor is internal, it
 * exposes no row set and its bound is only the minimal enclosing range; and it is mutable with a
 * transient builder. The test that cannot rot into a false claim is the one that checks no such
 * field exists.
 */
@Timeout(30)
class BigtableSourceConfigTest {

    @Test
    void holdsNoQueryOfTheClientsOwn() {
        assertThat(fieldTypesOf(BigtableSourceConfig.class)).doesNotContain(Query.class);
        assertThat(fieldTypesOf(RowRangeSplit.class)).doesNotContain(Query.class);
    }

    @Test
    void copiesTheRangesItWasGivenAndTheOnesItHandsOut() {
        BigtableSourceConfig<String> config =
                TestSources.config(builder -> builder.rowRange("a", "m"));

        assertThat(config.getRanges()).isNotSameAs(config.getRanges());
        assertThat(config.getRanges().get(0)).isNotSameAs(config.getRanges().get(0));
    }

    @Test
    void carriesTheSeamsTheEnumeratorAndTheReadersUse() {
        BigtableSourceConfig<String> config = TestSources.config();

        assertThat(config.getSampler()).isNotNull();
        assertThat(config.getOpener()).isNotNull();
    }

    private static java.util.List<Class<?>> fieldTypesOf(Class<?> type) {
        java.util.List<Class<?>> types = new java.util.ArrayList<>();
        for (Field field : type.getDeclaredFields()) {
            types.add(field.getType());
        }
        return types;
    }
}
