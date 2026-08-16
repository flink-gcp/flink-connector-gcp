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

package io.github.flink.gcp.connector.spanner;

import com.google.cloud.spanner.Options.RpcPriority;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SpannerRpcPriority}. */
class SpannerRpcPriorityTest {

    @Test
    void eachValueMapsOntoTheClientLibrarysOwn() {
        // Asserted value by value rather than by name, for the same reason the mapping is a
        // written-out switch: a transposition here would send every LOW job's reads at HIGH, and
        // nothing downstream — not the emulator, which ignores the priority, and not a green
        // production job — would ever say so.
        assertThat(SpannerRpcPriority.LOW.toSpanner()).isEqualTo(RpcPriority.LOW);
        assertThat(SpannerRpcPriority.MEDIUM.toSpanner()).isEqualTo(RpcPriority.MEDIUM);
        assertThat(SpannerRpcPriority.HIGH.toSpanner()).isEqualTo(RpcPriority.HIGH);
    }

    @ParameterizedTest
    @EnumSource(SpannerRpcPriority.class)
    void everyValueMapsToSomething(SpannerRpcPriority priority) {
        // A value added to this enum and forgotten in the switch reaches the default arm, which
        // throws — so this fails at the moment the enum grows rather than on a job that used it.
        assertThat(priority.toSpanner()).isNotNull();
    }

    @Test
    void theEnumCoversEveryPriorityLevelTheClientHasExceptUnspecified() {
        // Not a style assertion: this enum exists to keep SDK types off the public API, and it can
        // only do that while it can express every level the client can. A level added on the
        // client's side is a knob this connector silently cannot offer until someone notices.
        //
        // UNSPECIFIED is the one deliberate omission. This connector expresses "unspecified" by
        // leaving the knob unset and sending no priority at all, which is what keeps the service's
        // own handling in place rather than freezing today's meaning of the sentinel into a job.
        assertThat(SpannerRpcPriority.values())
                .extracting(Enum::name)
                .containsExactlyInAnyOrderElementsOf(
                        Arrays.stream(RpcPriority.values())
                                .map(Enum::name)
                                .filter(name -> !"UNSPECIFIED".equals(name))
                                .collect(Collectors.toList()));
    }
}
