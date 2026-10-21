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

package io.github.flink.gcp.connector.tier3.spanner;

import org.apache.flink.api.java.tuple.Tuple3;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SeedCopyTest {

    @Test
    void aBatchRowIsKeyedByKeyAndDeliveryAndHoldsTheSequence() {
        Mutation mutation = SeedCopy.mutation(Tuple3.of("a/0000042", "upgrade/u/3", 42L));
        assertThat(mutation.getOperation()).isEqualTo(Mutation.Op.INSERT_OR_UPDATE);
        assertThat(mutation.getTable()).isEqualTo("batch_out");
        assertThat(mutation.asMap())
                .containsOnly(
                        Map.entry("k", Value.string("a/0000042")),
                        Map.entry("delivery", Value.string("upgrade/u/3")),
                        Map.entry("n", Value.int64(42)));
    }
}
