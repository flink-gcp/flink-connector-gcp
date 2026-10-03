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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RampUpThrottleTest {

    private static final long MINUTE = 60_000;

    private final AtomicLong clock = new AtomicLong(1_000_000);
    private final List<Long> sleeps = new ArrayList<>();

    @Test
    void theSinkStartsAtFiveHundredSharedAmongItsSubtasks() {
        assertThat(RampUpThrottle.budget(0, 1)).isEqualTo(500);
        assertThat(RampUpThrottle.budget(0, 4)).isEqualTo(125);
        assertThat(RampUpThrottle.budget(0, 1000)).isEqualTo(1);
    }

    @Test
    void theBudgetGrowsByHalfEveryFiveMinutesAfterTheFirstFive() {
        assertThat(RampUpThrottle.budget(5 * MINUTE, 1)).isEqualTo(500);
        assertThat(RampUpThrottle.budget(10 * MINUTE, 1)).isEqualTo(750);
        assertThat(RampUpThrottle.budget(15 * MINUTE, 1)).isEqualTo(1125);
        // Continuous growth at whole-minute steps, as Beam computes it.
        assertThat(RampUpThrottle.budget(6 * MINUTE, 1))
                .isEqualTo((int) (500 * Math.pow(1.5, 0.2)));
        assertThat(RampUpThrottle.budget(6 * MINUTE - 1, 1)).isEqualTo(500);
        assertThat(RampUpThrottle.budget(1000 * MINUTE, 1)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void aSpentWindowWaitsForTheNextOne() throws Exception {
        RampUpThrottle throttle = throttle(250);

        assertThat(throttle.acquire()).isZero();
        assertThat(throttle.acquire()).isZero();
        clock.addAndGet(400);
        assertThat(throttle.acquire()).isEqualTo(600);
        assertThat(sleeps).containsExactly(600L);
        assertThat(throttle.acquire()).isZero();
        assertThat(throttle.acquire()).isEqualTo(1000);
    }

    @Test
    void aClockSteppingBackwardsOpensANewWindowRatherThanStalling() throws Exception {
        RampUpThrottle throttle = throttle(500);
        throttle.acquire();
        clock.addAndGet(-10_000);

        assertThat(throttle.acquire()).isZero();
        assertThat(sleeps).isEmpty();
    }

    @Test
    void aNegativeMonotonicReadingIsAnOrdinaryStart() throws Exception {
        clock.set(-5_000_000);
        RampUpThrottle throttle = throttle(500);

        assertThat(throttle.acquire()).isZero();
        assertThat(throttle.acquire()).isEqualTo(1000);
        assertThat(throttle.acquire()).isEqualTo(1000);
        assertThat(sleeps).containsExactly(1000L, 1000L);
    }

    @Test
    void aParallelismMustBePositive() {
        assertThatThrownBy(() -> new RampUpThrottle(0, clock::get, millis -> {}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private RampUpThrottle throttle(int parallelism) {
        return new RampUpThrottle(
                parallelism,
                clock::get,
                millis -> {
                    sleeps.add(millis);
                    clock.addAndGet(millis);
                });
    }
}
