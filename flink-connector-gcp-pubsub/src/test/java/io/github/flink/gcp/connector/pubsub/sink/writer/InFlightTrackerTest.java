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

package io.github.flink.gcp.connector.pubsub.sink.writer;

import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherOptions;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link InFlightTracker} — the two waits, the caps that end them, and the progress
 * budget that fails them.
 *
 * <p>The writer's own tests drive these paths through a whole sink; these drive them directly,
 * which is what makes the once-per-wait flush and the budget's start-of-wait arithmetic separable
 * from everything else a publish touches.
 */
@Timeout(30)
class InFlightTrackerTest {

    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final AtomicInteger flushes = new AtomicInteger();

    @Test
    void aWaitEndsWhenACompletionBringsTheCountBackUnderTheCap() throws Exception {
        InFlightTracker tracker = tracker(options(2, 1 << 20, Duration.ofSeconds(30)));
        tracker.admit(10);
        tracker.admit(10);
        // The mail a completion would post: the wait runs it, which is what lets the wait end.
        mailbox.execute(() -> tracker.release(10), "completion");

        tracker.awaitCapacity();

        assertThat(tracker.getInFlightMessages()).isEqualTo(1);
        assertThat(tracker.getInFlightBytes()).isEqualTo(10L);
    }

    @Test
    void anEmptyTrackerAdmitsWithoutWaitingEvenForAMessageLargerThanTheCap() {
        InFlightTracker tracker = tracker(options(1000, 8, Duration.ofSeconds(30)));

        // "At or above the cap", never "would this message fit": a wait ends only when a publish
        // completes, so a predicate that can hold at zero would wait for what cannot happen.
        assertThatCode(tracker::awaitCapacity).doesNotThrowAnyException();

        tracker.admit(1024);
        assertThat(tracker.getInFlightBytes()).isEqualTo(1024L);
    }

    @Test
    void theByteCapEndsAWaitOnItsOwn() throws Exception {
        InFlightTracker tracker = tracker(options(1000, 100, Duration.ofSeconds(30)));
        tracker.admit(100);
        mailbox.execute(() -> tracker.release(100), "completion");

        tracker.awaitCapacity();

        assertThat(tracker.getInFlightBytes()).isZero();
    }

    @Test
    void theDrainRunsToZeroRatherThanToTheCap() throws Exception {
        InFlightTracker tracker = tracker(options(1000, 1 << 20, Duration.ofSeconds(30)));
        tracker.admit(1);
        tracker.admit(1);
        mailbox.execute(() -> tracker.release(1), "first");
        mailbox.execute(() -> tracker.release(1), "second");

        tracker.drainToEmpty();

        assertThat(tracker.getInFlightMessages()).isZero();
    }

    @Test
    void theDrainKeepsGoingForAMessageThatSerializesToNoBytes() throws Exception {
        InFlightTracker tracker = tracker(options(1000, 1 << 20, Duration.ofSeconds(30)));
        // Keyed on the count alone: zero bytes in flight does not mean an empty writer.
        tracker.admit(0);
        mailbox.execute(() -> tracker.release(0), "completion");

        tracker.drainToEmpty();

        assertThat(tracker.getInFlightMessages()).isZero();
        assertThat(tracker.getInFlightBytes()).isZero();
    }

    @Test
    void onlyTheCapacityWaitAsksForWhatIsStillBatched() throws Exception {
        InFlightTracker tracker = tracker(options(1, 1 << 20, Duration.ofSeconds(30)));

        // Both waits must find the mailbox empty at least once, or neither would flush whatever
        // the code said and the comparison below would be 0 == 0.
        tracker.admit(1);
        completeAfterAPause(tracker, 1);
        tracker.awaitCapacity();
        int afterCapacityWait = flushes.get();
        assertThat(afterCapacityWait).isEqualTo(1);

        tracker.admit(1);
        completeAfterAPause(tracker, 1);
        tracker.drainToEmpty();

        // The drain's callers flush before calling it; doing it here too would repeat that flush.
        assertThat(flushes.get()).isEqualTo(afterCapacityWait);
    }

    @Test
    void theFlushHappensAtMostOncePerCapacityWait() throws Exception {
        // The flush itself is the signal that the wait has reached an empty-mailbox pass, so the
        // completer is released from inside it. No sleep decides anything: without this the test
        // races the scheduler in both directions — a completion queued before the first tryYield
        // gives zero flushes, and a slow completer gives many passes but must still give one.
        CountDownLatch reachedAnEmptyPass = new CountDownLatch(1);
        InFlightTracker tracker =
                new InFlightTracker(
                        mailbox,
                        options(1, 1 << 20, Duration.ofSeconds(30)),
                        LoggerFactory.getLogger(PubSubWriter.class),
                        () -> {},
                        () -> {
                            flushes.incrementAndGet();
                            reachedAnEmptyPass.countDown();
                        });
        tracker.admit(1);
        Thread completer =
                new Thread(
                        () -> {
                            await(reachedAnEmptyPass);
                            // Several more empty passes follow before this lands, which is what a
                            // per-pass flush would count.
                            sleep(50);
                            mailbox.execute(() -> tracker.release(1), "completion");
                        });
        completer.start();

        try {
            tracker.awaitCapacity();
        } finally {
            completer.join();
        }

        // Nothing can join a batch while the task thread is parked in the wait, so once is enough.
        assertThat(flushes.get()).isEqualTo(1);
    }

    @Test
    void aWaitThatKeepsRunningMailsNeverAsksForWhatIsStillBatched() throws Exception {
        InFlightTracker tracker = tracker(options(1, 1 << 20, Duration.ofSeconds(30)));
        tracker.admit(1);
        // A mail is already queued, so tryYield runs it and the wait ends without ever finding the
        // mailbox empty — flushing per record is exactly what a working batcher must not suffer.
        mailbox.execute(() -> tracker.release(1), "completion");

        tracker.awaitCapacity();

        assertThat(flushes.get()).isZero();
    }

    @Test
    void aStalledWaitFailsWithTheBudgetAndTheCountItWasHolding() {
        InFlightTracker tracker = tracker(options(1, 1 << 20, Duration.ofMillis(120)));
        tracker.admit(1);

        assertThatThrownBy(tracker::awaitCapacity)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("No publish to Pub/Sub completed for")
                .hasMessageContaining("while admitting a record")
                .hasMessageContaining("(1 publish(es) still in flight)")
                .hasMessageContaining("PT0.12S")
                .hasMessageContaining("bounds a publisher that has stopped answering, not a slow");
    }

    @Test
    void aCompletionRestartsTheBudgetSoASlowPublisherIsNotAStalledOne() throws Exception {
        // Two gaps, neither of them a stall: the publisher answers, then answers again. A deadline
        // on the call would end this wait; a budget restarted by every completion does not.
        //
        // The gaps are bounded below by the budget itself rather than by sleeps: the completer
        // waits for the wait to reach an empty-mailbox pass, then spends longer than the budget in
        // two halves. So the wait provably outlasts its own budget — a test whose sleeps were both
        // skipped could not pass — and no margin has to be guessed.
        Duration budget = Duration.ofMillis(300);
        CountDownLatch reachedAnEmptyPass = new CountDownLatch(1);
        InFlightTracker tracker =
                new InFlightTracker(
                        mailbox,
                        options(1, 1 << 20, budget),
                        LoggerFactory.getLogger(PubSubWriter.class),
                        () -> {},
                        reachedAnEmptyPass::countDown);
        tracker.admit(1);
        Thread completer =
                new Thread(
                        () -> {
                            await(reachedAnEmptyPass);
                            sleep(budget.toMillis() * 2 / 3);
                            tracker.recordCompletion();
                            sleep(budget.toMillis() * 2 / 3);
                            mailbox.execute(() -> tracker.release(1), "completion");
                        });
        long startedNanos = System.nanoTime();
        completer.start();
        try {
            assertThatCode(tracker::awaitCapacity).doesNotThrowAnyException();
        } finally {
            completer.join();
        }

        // The control: the wait lasted longer than the budget it did not spend.
        assertThat(System.nanoTime() - startedNanos).isGreaterThan(budget.toNanos());
    }

    @Test
    void bothWaitsSurfaceACapturedFailureEvenWithNothingInFlight() {
        AtomicInteger checks = new AtomicInteger();
        IOException captured = new IOException("captured on a callback thread");
        InFlightTracker tracker =
                new InFlightTracker(
                        mailbox,
                        options(1000, 1 << 20, Duration.ofSeconds(30)),
                        LoggerFactory.getLogger(PubSubWriter.class),
                        () -> {
                            checks.incrementAndGet();
                            throw captured;
                        },
                        flushes::incrementAndGet);

        // Nothing is in flight, so neither wait loops at all — and both still surface the failure
        // the callback captured, which is what stops a caller publishing past it. The in-loop
        // checks are exercised by the waits that do loop, above.
        assertThatThrownBy(tracker::awaitCapacity).isSameAs(captured);
        assertThatThrownBy(tracker::drainToEmpty).isSameAs(captured);
        assertThat(checks.get()).isEqualTo(2);
    }

    @Test
    void anInterruptEndsAWaitAsAnInterruptRatherThanAsATimeout() {
        InFlightTracker tracker = tracker(options(1, 1 << 20, Duration.ofSeconds(30)));
        tracker.admit(1);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(tracker::awaitCapacity)
                    .isInstanceOf(InterruptedException.class)
                    .hasMessageContaining("Interrupted while admitting a record");
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void aStalledWaitWarnsUnderTheWritersLogCategory() throws Exception {
        // The tracker is handed the writer's Logger precisely so this line keeps arriving where
        // operators filter for it, and where the writer's own tests assert on it. Verified here
        // against a bare tracker, which is the only way to see that the category survives the
        // extraction rather than the writer happening to log something itself.
        InFlightTracker tracker = tracker(options(1, 1 << 20, Duration.ofMillis(200)));
        tracker.admit(1);

        try (LogCapture capture = LogCapture.of(PubSubWriter.class)) {
            assertThatThrownBy(tracker::awaitCapacity).isInstanceOf(IOException.class);

            assertThat(capture.getMessages())
                    .anyMatch(message -> message.contains("No publish to Pub/Sub has completed"));
        }
    }

    @Test
    void sequencesAreHandedOutInIssueOrder() {
        InFlightTracker tracker = tracker(options(1000, 1 << 20, Duration.ofSeconds(30)));

        // What a parked batch is sorted by, so it has to be monotonic and gapless from zero.
        assertThat(tracker.nextSequence()).isZero();
        assertThat(tracker.nextSequence()).isEqualTo(1L);
        assertThat(tracker.nextSequence()).isEqualTo(2L);
    }

    @Test
    void bothCapsAreCheckedWhereTheyAreReliedOn() {
        // The builder rejects a non-positive cap too, with the same message — so passing one
        // through the builder would test the builder and never reach this constructor. These
        // guards exist for the instance the builder never ran on: an old serialized stream. The
        // field has to be forged to reach them, exactly as PubSubWriterProgressTimeoutTest does
        // for the budget.
        assertThatThrownBy(() -> tracker(forge("maxInFlightMessages", 0)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxInFlightMessages must be positive");
        assertThatThrownBy(() -> tracker(forge("maxInFlightBytes", 0L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxInFlightBytes must be positive");
    }

    /** Sets a field past the builder, as an options instance predating it would carry. */
    private static PubSubPublisherOptions forge(String name, Object value) {
        PubSubPublisherOptions options =
                PubSubPublisherOptions.builder()
                        .publishProgressTimeout(Duration.ofSeconds(30))
                        .build();
        try {
            Field field = PubSubPublisherOptions.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(options, value);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
        return options;
    }

    // The budget's own floor and ceiling (ADR-0068) are not reachable from here: the builder
    // rejects a zero or over-long duration before this class sees it, and the instance those
    // preconditions exist for is a deserialized one, which only
    // PubSubWriterProgressTimeoutTest.aNonPositiveBudgetIsRejectedWhereItIsReliedOn and
    // aBudgetTooLargeForNanosecondsIsRejectedWhereItIsReliedOn can forge. They reach this
    // constructor through the writer, which builds the tracker eagerly for that reason.

    private InFlightTracker tracker(PubSubPublisherOptions options) {
        return new InFlightTracker(
                mailbox,
                options,
                LoggerFactory.getLogger(PubSubWriter.class),
                () -> {},
                flushes::incrementAndGet);
    }

    private static PubSubPublisherOptions options(
            int maxInFlightMessages, long maxInFlightBytes, Duration publishProgressTimeout) {
        return PubSubPublisherOptions.builder()
                .maxInFlightMessages(maxInFlightMessages)
                .maxInFlightBytes(maxInFlightBytes)
                .publishProgressTimeout(publishProgressTimeout)
                .build();
    }

    /** Releases one publish after a pause long enough that the wait finds the mailbox empty. */
    private void completeAfterAPause(InFlightTracker tracker, int serializedSize) {
        Thread completer =
                new Thread(
                        () -> {
                            sleep(50);
                            mailbox.execute(() -> tracker.release(serializedSize), "completion");
                        });
        completer.setDaemon(true);
        completer.start();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new AssertionError("the wait never reached an empty-mailbox pass");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
