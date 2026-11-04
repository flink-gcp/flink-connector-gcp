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

package io.github.flink.gcp.connector.pubsub.source;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link SubscriberBufferLimits}. */
class SubscriberBufferLimitsTest {

    @Test
    void anUnsetBudgetScalesTheLapseBoundBySubscribers() {
        SubscriberBufferLimits limits =
                SubscriberBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(500)
                                .flowControlMaxOutstandingRequestBytes(1_000)
                                .build());

        assertThat(limits.lapseBoundMessages()).isEqualTo(1_000);
        assertThat(limits.lapseBoundBytes()).isEqualTo(2_000);
        assertThat(limits.maxMessages(3)).isEqualTo(3_000);
        assertThat(limits.maxBytes(3)).isEqualTo(6_000);
        // Fewer than one counts as one, so a reader with nothing registered still admits a share.
        assertThat(limits.maxMessages(0)).isEqualTo(1_000);
        assertThat(limits.coversDerived(3)).isTrue();
    }

    @Test
    void anExplicitPausedSplitBoundAboveTheLapseBoundBecomesTheShareButNotTheLapseBound() {
        SubscriberBufferLimits limits =
                SubscriberBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(500)
                                .pausedSplitBufferMaxMessages(10_000)
                                .pausedSplitBufferMaxBytes(1)
                                .build());

        assertThat(limits.derivedMaxMessages(2)).isEqualTo(20_000);
        // Below the lapse bound the paused-split bound does not lower the share.
        assertThat(limits.derivedMaxBytes(1)).isEqualTo(limits.lapseBoundBytes());
        // The paused-split default stays the lapse bound itself.
        assertThat(limits.lapseBoundMessages()).isEqualTo(1_000);
    }

    @Test
    void scalingSaturatesInsteadOfWrapping() {
        SubscriberBufferLimits limits =
                SubscriberBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(Long.MAX_VALUE / 2 - 1)
                                .flowControlMaxOutstandingRequestBytes(Long.MAX_VALUE)
                                .build());

        assertThat(limits.maxMessages(1)).isEqualTo(Long.MAX_VALUE - 3);
        assertThat(limits.maxMessages(2)).isEqualTo(Long.MAX_VALUE);
        assertThat(limits.maxBytes(1)).isEqualTo(Long.MAX_VALUE);
        assertThat(limits.maxBytes(Integer.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void anExplicitCapIsNotScaledAndReportsWhetherItCoversTheDerivedCap() {
        SubscriberBufferLimits limits =
                SubscriberBufferLimits.of(
                        PubSubSubscriberOptions.builder()
                                .flowControlMaxOutstandingElementCount(500)
                                .subscriberBufferMaxMessages(2_500)
                                .build());

        assertThat(limits.maxMessages(1)).isEqualTo(2_500);
        assertThat(limits.maxMessages(3)).isEqualTo(2_500);
        assertThat(limits.coversDerived(2)).isTrue();
        assertThat(limits.coversDerived(3)).isFalse();
    }

    @Test
    void fixedLimitsAreNeverBelowTheirDerivedCap() {
        SubscriberBufferLimits limits = SubscriberBufferLimits.fixed(4, 5);

        assertThat(limits.maxMessages(100)).isEqualTo(4);
        assertThat(limits.maxBytes(100)).isEqualTo(5);
        assertThat(limits.coversDerived(100)).isTrue();
    }

    @Test
    void anUnsetBudgetIsNotCheckedAgainstAnything() {
        assertThatCode(
                        () ->
                                SubscriberBufferLimits.checkCoversFlowControl(
                                        null, null, 1L, 1L, SubscriberBufferLimits.Names.BUILDER))
                .doesNotThrowAnyException();
    }
}
