/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.pubsub.source.AbstractPubSubSourceEmulatorITCase;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.ACK_DEADLINE_SECONDS;
import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.BACKLOG;
import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.FLOW_CONTROL_MESSAGES;
import static io.github.flink.gcp.connector.pubsub.source.streamingpull.reader.BackpressuredArm.REFILL_RATE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures whether the ack-extension lapse grows a <em>backpressured</em> split's buffer the way it
 * grows a paused one, and what decides how far ([#377]).
 *
 * <p><b>This class measures a premise rather than covering a feature</b>, as {@link
 * PubSubPausedSplitBufferITCase} does for the paused case. What #357 established is that a split
 * nothing drains outgrows flow control once the client library starts releasing permits it can no
 * longer extend leases for. A backpressured split differs in one respect that the paused case
 * cannot answer for it: it <em>is</em> drained, whenever the fetch loop gets to run at all.
 *
 * <p>The model the arms are placed around, derived from the permit accounting rather than assumed.
 * A delivery costs a flow-control permit, and permits come back from only two places: a settle, and
 * {@code AckHandler.forget()} at expiry. This source acknowledges nothing until a checkpoint
 * completes, and a checkpoint only ever covers messages the fetch loop already drained — so the
 * acknowledgement channel is <em>drain-neutral</em>, and the only source of permits that does not
 * follow the drain is expiry, at roughly {@link BackpressuredArm#REFILL_RATE}. A buffer should
 * therefore grow while the drain is under that rate and not while it is over: about 0.28 messages a
 * second at the production defaults of 1000 and one hour.
 *
 * <p>Three arms, run concurrently on three subscriptions so they share one emulator, one SDK and
 * one wall clock:
 *
 * <ul>
 *   <li><b>starved</b> never fetches. It is the firing control: growth has to be observable in this
 *       trial before a plateau anywhere else means anything. It is deliberately <em>not</em> the
 *       paused path — that nothing about pausing is required, only a stalled drain, is part of what
 *       is being measured.
 *   <li><b>slow</b> drains at a fifth of the refill rate. This is what separates #377 from #357: a
 *       demonstrably live drain that still loses ground.
 *   <li><b>fast</b> drains at three times it, and carries a second assertion — that its subscriber
 *       went on delivering — because a client that died would produce the same flat line.
 * </ul>
 *
 * <p>The instrument is {@link NotifyingPullSubscriber#bufferUsage()} rather than the {@code
 * messagesReceived} counter the paused class uses: that counter equals the deque only for a split
 * nothing drains, which is exactly the arm this class adds.
 *
 * <p>The reader's own paused-split bound (#357) cannot interfere and is held out of the way anyway:
 * {@code parkOverfullPausedSplits} returns immediately when no split is paused, and only ever reads
 * a paused split's usage. That a non-paused split's buffer is read by nothing at all is the other
 * half of this measurement.
 *
 * <p><b>Measured 2026-08-09, six runs against the emulator and {@code google-cloud-pubsub}
 * 1.152.0.</b> All three arms were delivered 151–195 messages over the 40 s window on one machine —
 * three to four times the 50-message flow-control window, whether the arm drained nothing, one
 * message a second or fifteen. What differed is what they were left holding: the stalled arm
 * 151–179, the slow arm 112–154 after draining 40, and the fast arm <b>zero</b>, its buffer peaking
 * at 46 in the first delivery and never rising again. The stalled arm's series repeats the paused
 * one ADR-0066 recorded (50, then ~101 at 10.3 s, then ~152 at 20.5 s), which is the finding stated
 * plainly: the lapse has nothing to do with pausing, only with nothing draining.
 *
 * <p>What the run does <em>not</em> establish is the delivery total, and two drafts have now
 * asserted one anyway. The first put a ceiling of {@code W × (1 + t/H)} on {@code messagesReceived}
 * and CI produced 327 against it: that counter includes redelivered copies, which cost no net
 * permit, so it is not the quantity the permit accounting bounds. The second barred the fast arm's
 * <em>drain</em> at half its requested rate — the same claim in another shape, since an arm can
 * only drain what it was given — and CI met that bar exactly, 300 drained against a bar of 300, on
 * a runner that delivered 318, 175 and 300 to arms this machine hands 151–195 (#440). The
 * break-even above is a rate of the right order, not a proven bound, and the arms are placed a
 * factor of five and three away from it for that reason.
 */
class PubSubBackpressuredSplitBufferITCase extends AbstractPubSubSourceEmulatorITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PubSubBackpressuredSplitBufferITCase.class);

    private static final double SLOW_RATE = REFILL_RATE / 5;
    private static final double FAST_RATE = REFILL_RATE * 3;

    /** Four permit holds, so the stalled arm sees several waves rather than one. */
    private static final Duration WINDOW = Duration.ofSeconds(40);

    /** How far above the window a buffer may sit and still be called bounded. */
    private static final double PLATEAU_TOLERANCE = 1.2;

    /**
     * How far past the window a buffer must go before it is called unbounded. Two waves is what the
     * emulator delivers — measured 50 → 101 → 153 and 50 → 115 → 170 for the stalled arm, then
     * flat, the same two-wave ceiling ADR-0066 recorded for a paused split — so the bar is one full
     * wave past the window rather than the two the mechanism predicts. Real Pub/Sub is the
     * authority on how many waves there are; what this establishes is that the drain rate decides
     * whether they accumulate.
     */
    private static final long GROWTH_FACTOR = 2;

    /** Half of what the sampler should manage, so a series cut short cannot read as a flat one. */
    private static final long MINIMUM_SAMPLES = WINDOW.toMillis() / 250 / 2;

    @Test
    void howFarABackpressuredSplitsBufferGrowsDependsOnItsDrainRate() throws Exception {
        BackpressuredArm starved = arm("bp-starved", 0);
        BackpressuredArm slow = arm("bp-slow", SLOW_RATE);
        BackpressuredArm fast = arm("bp-fast", FAST_RATE);
        List<BackpressuredArm> arms = List.of(starved, slow, fast);

        BackpressuredArm.runFor(WINDOW, arms);

        for (BackpressuredArm arm : arms) {
            LOG.info("Backpressured split buffer (#377): {}", arm);
            // A sampler that died early leaves every peak and plateau below reading as a flat line
            // it never observed.
            assertThat(arm.samples())
                    .as("the series was sampled throughout, %s", arm)
                    .isGreaterThanOrEqualTo(MINIMUM_SAMPLES);
        }

        assertThat(starved.buffered())
                .as("the firing control: a stalled drain outgrows flow control, %s", starved)
                .isGreaterThan(GROWTH_FACTOR * FLOW_CONTROL_MESSAGES);
        assertThat(starved.buffered())
                .as("and a stalled arm holds every message it was given, %s", starved)
                .isEqualTo(starved.received());

        assertThat(slow.drained())
                .as("the slow arm was demonstrably draining, %s", slow)
                .isGreaterThanOrEqualTo((long) (0.5 * SLOW_RATE * WINDOW.toSeconds()));
        assertThat(slow.buffered())
                .as("and its buffer is still past flow control in spite of it, %s", slow)
                .isGreaterThan(FLOW_CONTROL_MESSAGES);

        assertThat(fast.peakBuffered())
                .as("a drain above the refill rate never lets the buffer past the window, %s", fast)
                .isLessThanOrEqualTo((long) (PLATEAU_TOLERANCE * FLOW_CONTROL_MESSAGES));
        // Below the window rather than exactly zero, which is what six runs observed: the last
        // sample is taken while the drain is still running, so the most it can catch is a wave
        // part-drained rather than one the teardown stranded — see BackpressuredArm.runFor.
        assertThat(fast.buffered())
                .as("and it ends below one window rather than merely bounded, %s", fast)
                .isLessThan(FLOW_CONTROL_MESSAGES);

        // The contrast the class is for: the two drained arms were delivered the same order of
        // messages, past the same flow-control window, and one of them is holding all of it.
        for (BackpressuredArm arm : List.of(slow, fast)) {
            assertThat(arm.received())
                    .as("delivery outgrew the flow-control window here too, %s", arm)
                    .isGreaterThan(GROWTH_FACTOR * FLOW_CONTROL_MESSAGES);
        }

        // Nothing bounds an arm's delivery from above, here or anywhere: a bar on the fast arm's
        // drain is such a bound in disguise, and the javadoc records what happened to the two that
        // were tried (#440).

        // The emulator never redelivers a lease-expired message (ADR-0066), so nothing supersedes a
        // message the connector still holds and nothing is nacked before the teardown. Asserted
        // rather than assumed, because that channel adds buffered messages at no permit cost — and
        // measuring it needs real Pub/Sub, which is PubSubBackpressuredSplitBufferRealGcpITCase.
        for (BackpressuredArm arm : arms) {
            assertThat(arm.nacked())
                    .as("no redelivery reached the emulator's arms, %s", arm)
                    .isZero();
            assertThat(arm.distinct())
                    .as("so every drained message is distinct, %s", arm)
                    .isEqualTo(arm.drained());
        }
    }

    private BackpressuredArm arm(String name, double ratePerSecond) throws Exception {
        SubscriptionDestination subscription =
                createTopicAndSubscription(name, ACK_DEADLINE_SECONDS);
        publish(name, BackpressuredArm.payloads(BACKLOG));
        return new BackpressuredArm(
                name, ratePerSecond, subscription, EmulatorEndpoint.parse(emulatorEndpoint()));
    }
}
