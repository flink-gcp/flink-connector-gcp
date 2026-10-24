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

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DeliveryTest {

    @Test
    void namesEachDeliveryByPhaseInstanceAndPosition() {
        var first = new Delivery("initial");
        first.open(null);
        var a = first.map(Tuple2.of("a/0000000", 5L));
        var b = first.map(Tuple2.of("a/0000000", 5L));
        var second = new Delivery("initial");
        second.open(null);
        var c = second.map(Tuple2.of("a/0000000", 5L));

        assertThat(a.f0).isEqualTo("a/0000000");
        assertThat(a.f2).isEqualTo(5L);
        assertThat(a.f1).matches("initial/[a-f0-9-]{36}/0");
        // A repeat within one instance, and one from another instance, each add a delivery.
        assertThat(b.f1).isEqualTo(a.f1.substring(0, a.f1.length() - 1) + "1");
        assertThat(c.f1).isNotEqualTo(a.f1).endsWith("/0");
    }
}
