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

import com.google.api.gax.rpc.StatusCode;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.OrderingMode;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.FailedRequest;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;
import io.github.flink.gcp.connector.testutils.FakeMailboxExecutor;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SingleRowRequestWriter} under {@link OrderingMode#PER_KEY}.
 *
 * <p>A record is {@code "row:value"} and becomes a scripted request for the row before the colon,
 * so two records can address one row. Timed out as a class: a buffered request that no turn ever
 * readies is a hang, not a failure.
 */
@Timeout(30)
class SingleRowRequestWriterRowOrderTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");
    private static final TableDestination OTHER_TABLE = TableDestination.of("p", "i", "events");

    private static final long STALL_WARN_AFTER_NANOS = Duration.ofHours(1).toNanos();

    private final FakeSingleRowClientFactory factory = new FakeSingleRowClientFactory();
    private final FakeMailboxExecutor mailbox = new FakeMailboxExecutor();
    private final TestSinkWriterMetricGroup metricGroup = TestSinkWriterMetricGroup.create();
    private final RecordingHandler handler = new RecordingHandler();

    /** The request each record becomes, created on first use so a test can script it after. */
    private final Map<String, ScriptedRowRequest> requests = new LinkedHashMap<>();

    /** Where the resolver sends each record; {@link #TABLE} unless a test says otherwise. */
    private final Map<String, TableDestination> destinations = new LinkedHashMap<>();

    @Test
    void startsARowsLaterRequestOnlyAfterItsEarlierRequestIsAnswered() throws Exception {
        SingleRowRequestWriter<String> writer = writer(perKey().build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        writer.write("r2:a", TestContexts.NO_OP);

        assertThat(request("r1:a").starts).isEqualTo(1);
        assertThat(request("r1:b").starts).isZero();
        assertThat(request("r2:a").starts).isEqualTo(1);
        assertThat(writer.getBufferedRequests()).isEqualTo(1);
        assertThat(metricGroup.<Integer>gaugeValue("bufferedRequests")).isEqualTo(1);

        request("r1:a").succeed();
        mailbox.drain();
        // The answer only readied r1:b; the writer starts it from its own code, here the next
        // record's admission.
        assertThat(request("r1:b").starts).isZero();
        writer.write("r3:a", TestContexts.NO_OP);

        assertThat(request("r1:b").starts).isEqualTo(1);
        assertThat(writer.getBufferedRequests()).isZero();
        assertThat(writer.getInFlight()).isEqualTo(3);
        // A held request is a record sent once, when it starts.
        assertThat(metricGroup.counterValue("numRecordsSend")).isEqualTo(4);
    }

    @Test
    void withoutTheModeARowsRequestsRunAtOnce() throws Exception {
        SingleRowRequestWriter<String> writer = writer(BigtableRequestOptions.builder().build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);

        assertThat(request("r1:a").starts).isEqualTo(1);
        assertThat(request("r1:b").starts).isEqualTo(1);
        assertThat(writer.getBufferedRequests()).isZero();
        assertThat(metricGroup.<Integer>gaugeValue("bufferedRequests")).isZero();
    }

    @Test
    void theSameRowKeyInAnotherTableIsAnotherRow() throws Exception {
        destinations.put("r1:b", OTHER_TABLE);
        SingleRowRequestWriter<String> writer = writer(perKey().build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);

        assertThat(request("r1:b").starts).isEqualTo(1);
        assertThat(request("r1:b").startedFor).isEqualTo(OTHER_TABLE);
    }

    @Test
    void aRoutedRequestEndsItsRowsTurn() throws Exception {
        SingleRowRequestWriter<String> writer = writer(perKey().build());
        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);

        request("r1:a").fail(StatusCode.Code.INVALID_ARGUMENT);
        mailbox.drain();
        writer.write("r2:a", TestContexts.NO_OP);

        assertThat(handler.handled).hasSize(1);
        assertThat(request("r1:b").starts).isEqualTo(1);
    }

    @Test
    void aRequestTheClientRefusesBeforeSendingEndsItsRowsTurn() throws Exception {
        // The synchronous refusal is routed inside the submission itself; its row must be free for
        // the next request at once, or that request is buffered behind a request never in flight.
        request("r1:a").startFailure = new IllegalArgumentException("both branches empty");
        SingleRowRequestWriter<String> writer = writer(perKey().build());

        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);

        assertThat(handler.handled).hasSize(1);
        assertThat(request("r1:b").starts).isEqualTo(1);
        assertThat(writer.getBufferedRequests()).isZero();
    }

    @Test
    void bufferedRequestsCountAgainstTheCap() throws Exception {
        // With r1:a in flight and r1:b buffered the writer is at a cap of two, so admitting r2:a
        // runs the mailbox, which ends r1:a's turn, and the wait starts r1:b. Were buffered
        // requests not counted, r2:a would be admitted with r1:a's answer still queued and r1:b
        // left unstarted.
        SingleRowRequestWriter<String> writer = writer(perKey().maxInFlightRequests(2).build());
        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        request("r1:a").succeed();

        writer.write("r2:a", TestContexts.NO_OP);

        assertThat(request("r1:b").starts).isEqualTo(1);
        assertThat(request("r2:a").starts).isEqualTo(1);
        assertThat(writer.getBufferedRequests()).isZero();
    }

    @Test
    void flushStartsEachBufferedRequestInTurn() throws Exception {
        SingleRowRequestWriter<String> writer = writer(perKey().build());
        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);
        writer.write("r1:c", TestContexts.NO_OP);

        // Each answer is given only once its request has started, from another thread, while the
        // flush waits; a flush that drained once and returned would leave r1:b and r1:c unsent.
        Thread answering =
                new Thread(
                        () -> {
                            for (String record : List.of("r1:a", "r1:b", "r1:c")) {
                                awaitStart(request(record));
                                request(record).succeed();
                            }
                        });
        answering.setDaemon(true);
        answering.start();

        writer.flush(false);

        // Read before the join, so a flush that returned early names the request it left unsent
        // rather than timing out in the join.
        assertThat(request("r1:c").starts).as("r1:c started").isEqualTo(1);
        assertThat(writer.getBufferedRequests()).isZero();
        assertThat(writer.getInFlight()).isZero();
        answering.join(5_000);
        assertThat(metricGroup.counterValue("requestsCompleted")).isEqualTo(3);
    }

    @Test
    void closeForgetsWhatIsBuffered() throws Exception {
        SingleRowRequestWriter<String> writer = writer(perKey().build());
        writer.write("r1:a", TestContexts.NO_OP);
        writer.write("r1:b", TestContexts.NO_OP);

        writer.close();

        assertThat(writer.getBufferedRequests()).isZero();
        assertThat(request("r1:b").starts).isZero();
    }

    private static BigtableRequestOptions.Builder perKey() {
        return BigtableRequestOptions.builder().orderingMode(OrderingMode.PER_KEY);
    }

    private ScriptedRowRequest request(String record) {
        return requests.computeIfAbsent(
                record,
                key ->
                        new ScriptedRowRequest(
                                RowOperation.CHECK_AND_MUTATE_ROW,
                                key.substring(0, key.indexOf(':'))));
    }

    private SingleRowRequestWriter<String> writer(BigtableRequestOptions options) {
        return new SingleRowRequestWriter<>(
                new SingleRowRequestConfig<>(
                        (element, context) -> destinations.getOrDefault(element, TABLE),
                        (element, context) -> request(element),
                        null,
                        options,
                        handler,
                        null,
                        null,
                        false,
                        true),
                factory,
                mailbox,
                metricGroup,
                System::nanoTime,
                STALL_WARN_AFTER_NANOS);
    }

    private static void awaitStart(ScriptedRowRequest request) {
        while (request.starts == 0) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** A handler that drops every request, recording what it saw. */
    private static final class RecordingHandler implements FailureHandler<FailedElement> {

        private final List<FailedRequest> handled = new ArrayList<>();

        @Override
        public void handle(FailedElement element) {
            handled.add((FailedRequest) element);
        }
    }
}
