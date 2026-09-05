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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.testutils.LogCapture;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link BigtableRequestFunction}: the ledger and the counters it keeps across the task
 * thread and the client threads, the one-shot settlement between an answer and Flink's timeout, the
 * completion the function owes on every timeout path, and the instance cap it meets without
 * waiting.
 */
@Timeout(30)
class BigtableRequestFunctionTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");
    private static final TableDestination OTHER_TABLE = TableDestination.of("p", "i", "events");
    private static final TableDestination SECOND_INSTANCE =
            TableDestination.of("p", "i2", "orders");

    private final FakeSingleRowClientFactory factory = new FakeSingleRowClientFactory();
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();
    private final Map<String, ScriptedRowRequest> requests = new LinkedHashMap<>();

    /**
     * Served before {@link #requests}, for two invocations that carry one input; a queued request
     * is not registered under its key, so {@link #request(String)} does not find it.
     */
    private final Deque<ScriptedRowRequest> queuedRequests = new ArrayDeque<>();

    private final Map<String, TableDestination> destinations = new LinkedHashMap<>();
    private final List<String> skipped = new ArrayList<>();
    private final Map<String, Exception> failingInputs = new LinkedHashMap<>();
    private final TestClock clock = new TestClock();
    @Nullable private TestFunction function;

    @AfterEach
    void closeTheFunction() throws Exception {
        if (function != null) {
            function.close();
        }
    }

    @Test
    void anAnswerCompletesTheResultFromTheClientThread() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();

        function.asyncInvoke("row-1", result.asResultFuture());

        assertThat(function.getInFlight()).isEqualTo(1);
        assertThat(result.isDone()).isFalse();
        Thread client = new Thread(() -> request("row-1").succeed(), "client");
        client.start();
        client.join();

        assertThat(result.results()).containsExactly("out:row-1:answer:row-1");
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_ACCEPTED)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED)).isEqualTo(1);
        assertThat(metricGroup.<Integer>gaugeValue(BigtableMetricNames.IN_FLIGHT_REQUESTS))
                .isZero();
        assertThat(metricGroup.<Integer>gaugeValue(BigtableMetricNames.ACTIVE_CLIENTS))
                .isEqualTo(1);
    }

    @Test
    void aRowLevelFailureFailsTheInputNamingTheReason() throws Exception {
        // No handler on this surface: a rejection the sink writer would route is a job failure
        // here, and the message says what the service said.
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());

        request("row-1").fail(StatusCode.Code.INVALID_ARGUMENT);

        assertThat(result.failure())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CheckAndMutateRow request to Bigtable table " + TABLE)
                .hasMessageContaining("rejected because the request is invalid (INVALID_ARGUMENT)");
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isEqualTo(1);
        assertThat(metricGroup.counterValue("errorClass", "INVALID_ARGUMENT", "errors"))
                .isEqualTo(1);
    }

    @Test
    void anAmbiguousFailureSaysTheServiceMayHaveAppliedIt() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());

        request("row-1").fail(StatusCode.Code.UNAVAILABLE);

        assertThat(result.failure())
                .hasMessageContaining("failed with UNAVAILABLE before the service answered")
                .hasMessageContaining("may or may not have applied it");
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isZero();
    }

    @Test
    void aDeadlineFailureCountsAsATimeout() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());

        request("row-1").fail(StatusCode.Code.DEADLINE_EXCEEDED);

        assertThat(result.failure()).hasMessageContaining("may or may not have applied it");
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isEqualTo(1);
    }

    @Test
    void aMissingTableFailsTheInput() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());

        request("row-1").fail(StatusCode.Code.NOT_FOUND);

        assertThat(result.failure())
                .hasMessageContaining("table or one of its column families does not exist");
    }

    @Test
    void theOperatorTimeoutCancelsTheRequestAndNamesBothDeadlines() throws Exception {
        TestFunction function = open(options().requestTimeout(Duration.ofSeconds(7)).build());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());

        function.timeout("row-1", result.asResultFuture());

        assertThat(request("row-1").future.isCancelled()).isTrue();
        assertThat(result.completions()).isEqualTo(1);
        assertThat(result.failure())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("did not complete within the async operator's timeout")
                .hasMessageContaining("BigtableRequestOptions.requestTimeout (PT7S)")
                .hasMessageContaining("set the operator timeout above it");
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED)).isZero();
    }

    @Test
    void twoRecordsThatAreOneInputInstanceKeepSeparateLedgerEntries() throws Exception {
        // Two records in flight can be one input instance — a boxed small Long, an interned
        // String, an enum constant — while Flink hands timeout() the record's own ResultFuture. The
        // ledger is keyed by that result, so the first record's timeout cancels the first record's
        // request alone, and the second's answer still reaches the second's result.
        TestFunction function = open(options());
        String input = "row-1";
        ScriptedRowRequest first = new ScriptedRowRequest(input);
        ScriptedRowRequest second = new ScriptedRowRequest(input);
        queuedRequests.add(first);
        queuedRequests.add(second);
        FakeResultFuture<String> firstResult = new FakeResultFuture<>();
        FakeResultFuture<String> secondResult = new FakeResultFuture<>();
        function.asyncInvoke(input, firstResult.asResultFuture());
        function.asyncInvoke(input, secondResult.asResultFuture());

        function.timeout(input, firstResult.asResultFuture());

        assertThat(first.future.isCancelled()).as("first request cancelled").isTrue();
        assertThat(second.future.isCancelled()).as("second request cancelled").isFalse();
        assertThat(firstResult.failure()).hasMessageContaining("async operator's timeout");
        assertThat(secondResult.isDone()).isFalse();
        assertThat(function.getInFlight()).isEqualTo(1);

        second.succeed();

        assertThat(secondResult.results()).containsExactly("out:row-1:answer:row-1");
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED)).isEqualTo(1);
    }

    @Test
    void anAnswerAfterTheTimeoutIsIgnored() throws Exception {
        // The one-shot settlement against the client's own race: timeout() settles the handle
        // and then cancels the future, and an answer that wins the future's completion in
        // between — modelled by a request whose future ignores the cancel — reaches the callback
        // with the handle taken, and completes nothing.
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        request("row-1").ignoreCancellation();
        function.asyncInvoke("row-1", result.asResultFuture());
        function.timeout("row-1", result.asResultFuture());

        request("row-1").succeed();

        assertThat(result.completions()).isEqualTo(1);
        assertThat(result.failure()).hasMessageContaining("async operator's timeout");
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED)).isZero();
    }

    @Test
    void aTimeoutAfterTheAnswerCancelsNothingAndCountsNothing() throws Exception {
        // The operator calls timeout() only while the result is open, which after an answer is
        // the moment between its completion and the timer reading that. The ledger is empty by
        // then, so the function fails the input as one with no request in flight — a completion
        // the operator's own guard drops — and cancels and counts nothing.
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());
        request("row-1").succeed();

        function.timeout("row-1", result.asResultFuture());

        assertThat(result.completions()).isEqualTo(2);
        assertThat(result.results()).containsExactly("out:row-1:answer:row-1");
        assertThat(result.failures()).hasSize(1);
        assertThat(result.failures().get(0)).hasMessageContaining("no Bigtable request in flight");
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED)).isEqualTo(1);
    }

    @Test
    void aTimeoutBetweenRetryAttemptsFailsTheInputNamingNoRequestInFlight() throws Exception {
        // Retry mode: the operator's delegator takes the first attempt's failure for a retry and
        // holds the result open through the backoff; the timeout firing there cancels the retry
        // and hands the function the same result with nothing in flight. Flink has no fallback of
        // its own, so the function must complete it — a silent return leaks the queue slot.
        TestFunction function = open(options().requestTimeout(Duration.ofSeconds(7)).build());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());
        request("row-1").fail(StatusCode.Code.UNAVAILABLE);

        function.timeout("row-1", result.asResultFuture());

        assertThat(result.failures()).hasSize(2);
        assertThat(result.failures().get(0)).hasMessageContaining("failed with UNAVAILABLE");
        assertThat(result.failures().get(1))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("no Bigtable request in flight")
                .hasMessageContaining("between attempts of its retry strategy")
                .hasMessageContaining("nothing was cancelled")
                .hasMessageContaining("BigtableRequestOptions.requestTimeout (PT7S)");
        assertThat(function.getInFlight()).isZero();
        // No request ended on this timeout: the attempt before it was already counted.
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isZero();
    }

    @Test
    void aTimeoutAfterASkippedAttemptFailsTheInputTheSameWay() throws Exception {
        // A skip completes with an empty collection, which a retry strategy's result predicate
        // may take for a retry; the timeout then arrives with no ledger entry either.
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        skipped.add("row-1");
        function.asyncInvoke("row-1", result.asResultFuture());

        function.timeout("row-1", result.asResultFuture());

        assertThat(result.results()).isEmpty();
        assertThat(result.failures()).hasSize(1);
        assertThat(result.failures().get(0)).hasMessageContaining("no Bigtable request in flight");
        assertThat(metricGroup.counterValue(BigtableMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isZero();
    }

    @Test
    void aTimeoutWhileAFailureIsBeingHandedOffReadsTheRequestAsAnswered() throws Exception {
        // The ledger entry outlives the hand-off: a timer firing after the answer settled the
        // handle and before its completion reached Flink finds the entry and loses the
        // settlement, so it reports the request as answered at the timeout, not as an input
        // between retry attempts, which is what an emptied ledger would have made it say. It
        // completes the result itself, as Flink's default timeout would, because in retry mode
        // the operator may already have taken the answer for a retry this timeout cancelled;
        // whichever completion the operator processes first stands. The request's own counters
        // are the answer's.
        TestFunction function = open(options().requestTimeout(Duration.ofSeconds(7)).build());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());
        result.onNextCompletion(() -> function.timeout("row-1", result.asResultFuture()));

        request("row-1").fail(StatusCode.Code.INVALID_ARGUMENT);

        assertThat(result.failure())
                .as("the timeout's completion")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("CheckAndMutateRow request to Bigtable table " + TABLE)
                .hasMessageContaining("answered as the async operator's timeout elapsed")
                .hasMessageContaining("BigtableRequestOptions.requestTimeout (PT7S)");
        assertThat(result.completions())
                .as("the timeout's completion, then the answer's")
                .isEqualTo(2);
        assertThat(result.failures().get(1))
                .hasMessageContaining("rejected because the request is invalid");
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isEqualTo(1);
    }

    @Test
    void aTimeoutWhileAnAnswerIsBeingHandedOffReadsTheRequestAsAnswered() throws Exception {
        // The same on the success path, which drops its ledger entry in a block of its own.
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());
        result.onNextCompletion(() -> function.timeout("row-1", result.asResultFuture()));

        request("row-1").succeed();

        assertThat(result.failure())
                .as("the timeout's completion")
                .hasMessageContaining("answered as the async operator's timeout elapsed");
        assertThat(result.completions())
                .as("the timeout's completion, then the answer's")
                .isEqualTo(2);
        assertThat(result.failures()).hasSize(1);
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED)).isEqualTo(1);
    }

    @Test
    void anAnsweredRequestFreesItsInstanceBeforeItsResultReachesFlink() throws Exception {
        // Flink can emit an answer's result and hand the function the next input before the
        // answering thread has returned from its hand-off. The in-flight counts are released
        // before the hand-off, so at the instance cap that next input finds the answered
        // instance idle and evicts it, instead of being refused as if its request were still in
        // flight.
        TestFunction function = open(options().maxActiveInstances(1).build());
        destinations.put("row-2", SECOND_INSTANCE);
        FakeResultFuture<String> first = new FakeResultFuture<>();
        FakeResultFuture<String> second = new FakeResultFuture<>();
        function.asyncInvoke("row-1", first.asResultFuture());
        first.onNextCompletion(() -> function.asyncInvoke("row-2", second.asResultFuture()));

        request("row-1").succeed();

        assertThat(second.failure()).as("second input refused").isNull();
        assertThat(factory.released).containsExactly(TABLE);
        assertThat(factory.created).containsExactly(TABLE, SECOND_INSTANCE);
        assertThat(metricGroup.counterValue(BigtableMetricNames.CAPACITY_EVICTIONS)).isEqualTo(1);
        // The first answer's hand-off went on over the evicted state and reached its result.
        assertThat(first.results()).containsExactly("out:row-1:answer:row-1");
        request("row-2").succeed();
        assertThat(second.results()).containsExactly("out:row-2:answer:row-2");
        assertThat(function.getInFlight()).isZero();
    }

    @Test
    void aRetryAttemptUnderTheSameResultKeepsItsOwnLedgerEntry() throws Exception {
        // Retry mode re-invokes asyncInvoke with the ResultFuture of the attempt before, and can
        // do so while that attempt's answer is between its hand-off and its ledger removal: the
        // removal must leave the new attempt's entry alone, or the timeout finds nothing to cancel
        // and
        // reports no request in flight while one is.
        TestFunction function = open(options());
        String input = "row-1";
        ScriptedRowRequest first = new ScriptedRowRequest(input);
        ScriptedRowRequest second = new ScriptedRowRequest(input);
        queuedRequests.add(first);
        queuedRequests.add(second);
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke(input, result.asResultFuture());
        result.onNextCompletion(() -> function.asyncInvoke(input, result.asResultFuture()));
        first.fail(StatusCode.Code.UNAVAILABLE);

        function.timeout(input, result.asResultFuture());

        assertThat(second.future.isCancelled()).as("second attempt cancelled").isTrue();
        assertThat(result.failures()).hasSize(2);
        assertThat(result.failures().get(0)).hasMessageContaining("failed with UNAVAILABLE");
        assertThat(result.failures().get(1))
                .hasMessageContaining("did not complete within the async operator's timeout");
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_TIMED_OUT)).isEqualTo(1);
    }

    @Test
    void aNullRequestSkipsTheInputWithoutLeasingAClient() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        skipped.add("row-1");

        function.asyncInvoke("row-1", result.asResultFuture());

        assertThat(result.results()).isEmpty();
        assertThat(factory.created).isEmpty();
        assertThat(metricGroup.counterValue(BigtableMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_ACCEPTED)).isZero();
    }

    @Test
    void aNullDestinationFailsTheInput() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        destinations.put("row-1", null);

        function.asyncInvoke("row-1", result.asResultFuture());

        assertThat(result.failure())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("resolved to null");
        assertThat(factory.created).isEmpty();
    }

    @Test
    void aRequestThatCannotBeBuiltFailsTheInputWithItsOwnException() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        IllegalArgumentException cause = new IllegalArgumentException("not a row");
        failingInputs.put("row-1", cause);

        function.asyncInvoke("row-1", result.asResultFuture());

        assertThat(result.failure()).isSameAs(cause);
        assertThat(function.getInFlight()).isZero();
    }

    @Test
    void aClientRefusingTheStartFailsTheInputAndCountsNothing() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        request("row-1").startFailure = new IllegalStateException("client is shut down");

        function.asyncInvoke("row-1", result.asResultFuture());

        assertThat(result.failure())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to start a CheckAndMutateRow request")
                .hasMessageContaining(TABLE.toString())
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_ACCEPTED)).isZero();
    }

    @Test
    void aResultMappingFailureFailsTheInputAndStillCountsTheAnsweredRequest() throws Exception {
        TestFunction function = open(options());
        function.resultFailure = new IllegalStateException("cannot map");
        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-1", result.asResultFuture());

        request("row-1").succeed();

        assertThat(result.failure()).isSameAs(function.resultFailure);
        assertThat(function.getInFlight()).isZero();
        // The service answered, so the request completed; what the mapping made of the answer is
        // the function's own failure, reported on the result and under no request counter.
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isZero();
    }

    @Test
    void anAnswerTheMailboxRefusesIsLoggedAtDebugAndDropped() throws Exception {
        // Flink's ResultFuture hands the completion to the task mailbox, which throws to the
        // client thread once the task is finishing or failing; an uncaught throw there would
        // reach gax's callback executor, not the job.
        TestFunction function = open(options());
        FakeResultFuture<String> completing = new FakeResultFuture<>();
        FakeResultFuture<String> failing = new FakeResultFuture<>();
        function.asyncInvoke("row-1", completing.asResultFuture());
        function.asyncInvoke("row-2", failing.asResultFuture());
        completing.rejectCompletions();
        failing.rejectCompletions();

        try (LogCapture logs =
                LogCapture.of(BigtableRequestFunction.class, LogCapture.Level.DEBUG)) {
            request("row-1").succeed();
            request("row-2").fail(StatusCode.Code.NOT_FOUND);

            assertThat(logs.getMessages())
                    .filteredOn(message -> message.contains("quiesced or closed"))
                    .hasSize(2)
                    .anySatisfy(message -> assertThat(message).startsWith("Complete a Bigtable"))
                    .anySatisfy(message -> assertThat(message).startsWith("Fail a Bigtable"));
        }
        assertThat(completing.completions()).isZero();
        assertThat(failing.completions()).isZero();
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED)).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isEqualTo(1);
    }

    @Test
    void closeCancelsWhatIsOutstandingAndTheGaugesReadZero() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> first = new FakeResultFuture<>();
        FakeResultFuture<String> second = new FakeResultFuture<>();
        function.asyncInvoke("row-1", first.asResultFuture());
        function.asyncInvoke("row-2", second.asResultFuture());

        function.close();

        assertThat(request("row-1").future.isCancelled()).isTrue();
        assertThat(request("row-2").future.isCancelled()).isTrue();
        // A cancellation at close is not an outcome to report: the task is going away.
        assertThat(first.isDone()).isFalse();
        assertThat(second.isDone()).isFalse();
        assertThat(metricGroup.<Integer>gaugeValue(BigtableMetricNames.IN_FLIGHT_REQUESTS))
                .isZero();
        assertThat(metricGroup.<Integer>gaugeValue(BigtableMetricNames.ACTIVE_CLIENTS)).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED)).isZero();
        assertThat(factory.closeCalls).isEqualTo(1);
    }

    @Test
    void anAnswerLandingDuringCloseIsNeitherEmittedNorCounted() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> first = new FakeResultFuture<>();
        FakeResultFuture<String> second = new FakeResultFuture<>();
        function.asyncInvoke("row-1", first.asResultFuture());
        function.asyncInvoke("row-2", second.asResultFuture());
        // A client thread answering one request after close set its flag and before it cancelled
        // that request: the window is entered from the other request's cancellation, the first
        // thing close does after the flag. The set close snapshots is unordered, so each answers
        // the other and whichever cancels first does the answering; the second listener finds a
        // cancelled future and sets nothing. The answer wins settle() — its own cancel then finds
        // a done future — and must stop there: the operator is going away, and a result completed
        // now would be submitted to a mailbox that may already be quiesced.
        request("row-1").future.addListener(() -> request("row-2").succeed(), Runnable::run);
        request("row-2").future.addListener(() -> request("row-1").succeed(), Runnable::run);

        function.close();

        // Exactly one was cancelled; the other was answered inside the window.
        assertThat(request("row-1").future.isCancelled())
                .isNotEqualTo(request("row-2").future.isCancelled());
        assertThat(first.isDone()).isFalse();
        assertThat(second.isDone()).isFalse();
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED)).isZero();
    }

    @Test
    void countersAreExactUnderConcurrentAnswers() throws Exception {
        // The reason the counters are thread-safe: answers arrive on client threads, and a plain
        // SimpleCounter would lose increments under the contention modelled here.
        int threads = 8;
        int perThread = 200;
        TestFunction function = open(options().maxInFlightRequests(threads * perThread).build());
        List<FakeResultFuture<String>> results = new ArrayList<>();
        for (int i = 0; i < threads * perThread; i++) {
            FakeResultFuture<String> result = new FakeResultFuture<>();
            results.add(result);
            function.asyncInvoke("row-" + i, result.asResultFuture());
        }
        assertThat(metricGroup.registeredCounter(BigtableMetricNames.REQUESTS_COMPLETED))
                .isInstanceOf(ThreadSafeSimpleCounter.class);

        CyclicBarrier start = new CyclicBarrier(threads);
        CountDownLatch done = new CountDownLatch(threads);
        for (int t = 0; t < threads; t++) {
            int from = t * perThread;
            new Thread(
                            () -> {
                                try {
                                    start.await();
                                    for (int i = from; i < from + perThread; i++) {
                                        if (i % 2 == 0) {
                                            request("row-" + i).succeed();
                                        } else {
                                            request("row-" + i)
                                                    .fail(StatusCode.Code.INVALID_ARGUMENT);
                                        }
                                    }
                                } catch (Exception e) {
                                    throw new AssertionError(e);
                                } finally {
                                    done.countDown();
                                }
                            },
                            "client-" + t)
                    .start();
        }
        done.await();

        assertThat(results).allMatch(FakeResultFuture::isDone);
        assertThat(function.getInFlight()).isZero();
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_COMPLETED))
                .isEqualTo(threads * perThread / 2);
        assertThat(metricGroup.counterValue(BigtableMetricNames.REQUESTS_FAILED))
                .isEqualTo(threads * perThread / 2);
        assertThat(metricGroup.counterValue("errorClass", "INVALID_ARGUMENT", "errors"))
                .isEqualTo(threads * perThread / 2);
    }

    @Test
    void anInstanceClientIsSharedByItsTablesAndLeasedPerTable() throws Exception {
        TestFunction function = open(options());
        destinations.put("row-2", OTHER_TABLE);
        function.asyncInvoke("row-1", new FakeResultFuture<String>().asResultFuture());
        function.asyncInvoke("row-2", new FakeResultFuture<String>().asResultFuture());

        assertThat(factory.created).containsExactly(TABLE, OTHER_TABLE);
        assertThat(request("row-1").startedOn).isSameAs(request("row-2").startedOn);
        assertThat(function.getActiveClients()).isEqualTo(1);
        request("row-1").succeed();
        request("row-2").succeed();
    }

    @Test
    void atTheInstanceCapAnIdleInstanceIsEvictedForTheNewOne() throws Exception {
        TestFunction function = open(options().maxActiveInstances(1).build());
        destinations.put("row-2", SECOND_INSTANCE);
        function.asyncInvoke("row-1", new FakeResultFuture<String>().asResultFuture());
        request("row-1").succeed();

        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-2", result.asResultFuture());

        assertThat(factory.released).containsExactly(TABLE);
        assertThat(factory.created).containsExactly(TABLE, SECOND_INSTANCE);
        assertThat(function.getActiveClients()).isEqualTo(1);
        assertThat(metricGroup.counterValue(BigtableMetricNames.CAPACITY_EVICTIONS)).isEqualTo(1);
        request("row-2").succeed();
        assertThat(result.results()).containsExactly("out:row-2:answer:row-2");
    }

    @Test
    void atTheInstanceCapABusyInstanceRefusesTheNewOneNamingTheOption() throws Exception {
        // The function cannot block for a drain the way the sink writer does, so the refusal is
        // deterministic and names the knob rather than overshooting the cap.
        TestFunction function = open(options().maxActiveInstances(1).build());
        destinations.put("row-2", SECOND_INSTANCE);
        function.asyncInvoke("row-1", new FakeResultFuture<String>().asResultFuture());

        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-2", result.asResultFuture());

        assertThat(result.failure())
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot open a Bigtable client for table " + SECOND_INSTANCE)
                .hasMessageContaining("BigtableRequestOptions.maxActiveInstances");
        assertThat(factory.created).containsExactly(TABLE);
        assertThat(factory.released).isEmpty();
        assertThat(function.getInFlight()).isEqualTo(1);
        request("row-1").succeed();
    }

    @Test
    void aTableOfAHeldInstanceNeedsNoEviction() throws Exception {
        TestFunction function = open(options().maxActiveInstances(1).build());
        destinations.put("row-2", OTHER_TABLE);
        function.asyncInvoke("row-1", new FakeResultFuture<String>().asResultFuture());

        FakeResultFuture<String> result = new FakeResultFuture<>();
        function.asyncInvoke("row-2", result.asResultFuture());

        assertThat(result.isDone()).isFalse();
        assertThat(factory.created).containsExactly(TABLE, OTHER_TABLE);
        assertThat(factory.released).isEmpty();
        request("row-1").succeed();
        request("row-2").succeed();
    }

    @Test
    void idleTablesAreSweptAsInputsArrive() throws Exception {
        // The surface has no flush to sweep from, so the sweep rides on the next input. The clock
        // is the test's, so "idle beyond the timeout" is a number and not a sleep.
        TestFunction function = open(options().destinationIdleTimeout(Duration.ofHours(1)));
        destinations.put("row-2", SECOND_INSTANCE);
        function.asyncInvoke("row-1", new FakeResultFuture<String>().asResultFuture());
        request("row-1").succeed();
        clock.advance(Duration.ofHours(1).plusNanos(1));

        function.asyncInvoke("row-2", new FakeResultFuture<String>().asResultFuture());

        assertThat(factory.released).containsExactly(TABLE);
        assertThat(metricGroup.counterValue(BigtableMetricNames.IDLE_EVICTIONS)).isEqualTo(1);
        assertThat(function.getActiveClients()).isEqualTo(1);
        request("row-2").succeed();
    }

    @Test
    void theSweepRunsAtMostOncePerIdleTimeout() throws Exception {
        // The sweep would otherwise walk the pool on every input. Between two sweeps a table can
        // cross the idle line and stay leased until the next one; a function that swept per input
        // evicts it at 90 minutes here, and one that never sweeps still holds it at 120.
        TestFunction function = open(options().destinationIdleTimeout(Duration.ofHours(1)));
        destinations.put("row-2", SECOND_INSTANCE);
        destinations.put("row-3", SECOND_INSTANCE);
        destinations.put("row-4", SECOND_INSTANCE);
        clock.advance(Duration.ofMinutes(10));
        function.asyncInvoke("row-1", new FakeResultFuture<String>().asResultFuture());
        request("row-1").succeed();

        // 60 minutes after open: a sweep, which finds the table 50 minutes idle and keeps it.
        clock.advance(Duration.ofMinutes(50));
        function.asyncInvoke("row-2", new FakeResultFuture<String>().asResultFuture());
        request("row-2").succeed();
        assertThat(factory.released).isEmpty();

        // 90 minutes: the table is 80 minutes idle, but the last sweep was 30 minutes ago.
        clock.advance(Duration.ofMinutes(30));
        function.asyncInvoke("row-3", new FakeResultFuture<String>().asResultFuture());
        request("row-3").succeed();
        assertThat(factory.released).isEmpty();
        assertThat(function.getActiveClients()).isEqualTo(2);

        // 120 minutes: the next sweep is due and the table goes.
        clock.advance(Duration.ofMinutes(30));
        function.asyncInvoke("row-4", new FakeResultFuture<String>().asResultFuture());
        request("row-4").succeed();

        assertThat(factory.released).containsExactly(TABLE);
        assertThat(metricGroup.counterValue(BigtableMetricNames.IDLE_EVICTIONS)).isEqualTo(1);
        assertThat(function.getActiveClients()).isEqualTo(1);
    }

    @Test
    void aTableWithARequestInFlightIsNotSweptHoweverIdle() throws Exception {
        TestFunction function = open(options().destinationIdleTimeout(Duration.ofHours(1)));
        destinations.put("row-2", SECOND_INSTANCE);
        function.asyncInvoke("row-1", new FakeResultFuture<String>().asResultFuture());
        clock.advance(Duration.ofHours(1).plusNanos(1));

        function.asyncInvoke("row-2", new FakeResultFuture<String>().asResultFuture());

        assertThat(factory.released).isEmpty();
        assertThat(function.getActiveClients()).isEqualTo(2);
        request("row-1").succeed();
        request("row-2").succeed();
    }

    @Test
    void aFailedClientCreationFailsTheInputNamingTheTable() throws Exception {
        TestFunction function = open(options());
        FakeResultFuture<String> result = new FakeResultFuture<>();
        factory.createFailures.add(new IOException("no channel"));

        function.asyncInvoke("row-1", result.asResultFuture());

        assertThat(result.failure()).isInstanceOf(IOException.class);
        assertThat(function.getInFlight()).isZero();
        assertThat(function.getActiveClients()).isZero();
    }

    @Test
    void aNonPositiveInstanceCapIsRejectedAtOpen() throws Exception {
        // A deserialized options instance did not run the builder; the guard the builder gave is
        // re-applied where the function comes to life.
        BigtableRequestOptions unchecked = corrupted(options().build());
        TestFunction function = new TestFunction(factory, unchecked);
        function.setRuntimeContext(runtimeContext());

        assertThatThrownBy(() -> function.open(DefaultOpenContext.INSTANCE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxActiveInstances must be positive");
    }

    private static BigtableRequestOptions corrupted(BigtableRequestOptions options)
            throws Exception {
        java.lang.reflect.Field field =
                BigtableRequestOptions.class.getDeclaredField("maxActiveInstances");
        field.setAccessible(true);
        field.setInt(options, 0);
        return options;
    }

    private static BigtableRequestOptions.Builder options() {
        return BigtableRequestOptions.builder();
    }

    private TestFunction open(BigtableRequestOptions.Builder options) throws Exception {
        return open(options.build());
    }

    private TestFunction open(BigtableRequestOptions options) throws Exception {
        TestFunction function = new TestFunction(factory, options);
        function.setRuntimeContext(runtimeContext());
        function.open(DefaultOpenContext.INSTANCE);
        this.function = function;
        return function;
    }

    private ScriptedRowRequest request(String rowKey) {
        return requests.computeIfAbsent(rowKey, ScriptedRowRequest::new);
    }

    /** The slice of a {@link RuntimeContext} the function touches: its metric group. */
    private RuntimeContext runtimeContext() {
        return (RuntimeContext)
                Proxy.newProxyInstance(
                        RuntimeContext.class.getClassLoader(),
                        new Class<?>[] {RuntimeContext.class},
                        (proxy, method, arguments) -> {
                            switch (method.getName()) {
                                case "getMetricGroup":
                                    return metricGroup;
                                case "getUserCodeClassLoader":
                                    return BigtableRequestFunctionTest.class.getClassLoader();
                                case "getGlobalJobParameters":
                                    return Collections.emptyMap();
                                case "isObjectReuseEnabled":
                                    return false;
                                case "toString":
                                    return "BigtableRequestFunctionTestRuntimeContext";
                                default:
                                    throw new UnsupportedOperationException(
                                            "Unexpected RuntimeContext call: " + method.getName());
                            }
                        });
    }

    /** Routes every input through the test's scripted requests and destinations. */
    private final class TestFunction extends BigtableRequestFunction<String, String, String> {

        private static final long serialVersionUID = 1L;

        @Nullable private RuntimeException resultFailure;

        private TestFunction(SingleRowClientFactory clientFactory, BigtableRequestOptions options) {
            super(clientFactory, options, clock);
        }

        @Override
        protected TableDestination destination(String input) {
            return destinations.containsKey(input) ? destinations.get(input) : TABLE;
        }

        @Override
        @Nullable
        protected RowRequest<String> request(String input) throws Exception {
            Exception failure = failingInputs.get(input);
            if (failure != null) {
                throw failure;
            }
            if (skipped.contains(input)) {
                return null;
            }
            ScriptedRowRequest queued = queuedRequests.poll();
            return queued != null ? queued : BigtableRequestFunctionTest.this.request(input);
        }

        @Override
        protected String result(
                String input,
                String answer,
                TableDestination destination,
                RowRequest<String> request) {
            if (resultFailure != null) {
                throw resultFailure;
            }
            return "out:" + input + ":" + answer;
        }
    }

    /** A nanosecond clock the test advances by hand; the function reads nothing else. */
    private static final class TestClock implements LongSupplier {

        private long nanos;

        @Override
        public long getAsLong() {
            return nanos;
        }

        void advance(Duration by) {
            nanos += by.toNanos();
        }
    }
}
