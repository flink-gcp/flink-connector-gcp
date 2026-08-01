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

package io.github.flink.gcp.connector.testutils.pubsub;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import com.google.pubsub.v1.PubsubMessage;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Fetch-loop helpers for driving a Pub/Sub {@link SplitReader} directly in tests, typed against the
 * {@code flink-connector-base} interface so the connector's reader class never crosses the module
 * boundary.
 */
@Internal
public final class PubSubSplitReaders {

    /**
     * Fetches until {@code expected} <em>distinct</em> messages have been collected or the timeout
     * elapses, returning them in arrival order. Distinct by split and message id — message ids are
     * only unique within a topic — because delivery is at-least-once: a wait long enough to span
     * the acknowledgement deadline can legitimately see the same message twice, and a duplicate
     * must dedupe rather than crowd out a message still to arrive. A fetch blocks until data
     * arrives, so a waker nudges the reader once the deadline passes; an emptiness check ({@code
     * expected = Integer.MAX_VALUE}) holds its full window.
     */
    public static List<PubsubMessage> fetchUntil(
            SplitReader<PubsubMessage, ?> reader, int expected, Duration timeout) throws Exception {
        Map<String, PubsubMessage> received = new LinkedHashMap<>();
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        ScheduledExecutorService waker = Executors.newSingleThreadScheduledExecutor();
        try {
            // Nudge the reader periodically so a fetch that has nothing to return cannot outlive
            // the deadline check.
            waker.scheduleAtFixedRate(reader::wakeUp, 200, 200, TimeUnit.MILLISECONDS);
            while (received.size() < expected && System.nanoTime() < deadlineNanos) {
                RecordsWithSplitIds<PubsubMessage> records = reader.fetch();
                String splitId;
                while ((splitId = records.nextSplit()) != null) {
                    PubsubMessage message;
                    while ((message = records.nextRecordFromSplit()) != null) {
                        received.putIfAbsent(splitId + "/" + message.getMessageId(), message);
                    }
                }
            }
        } finally {
            waker.shutdownNow();
        }
        return new ArrayList<>(received.values());
    }

    /** Returns the messages' UTF-8 payloads, in order. */
    public static List<String> payloads(List<PubsubMessage> messages) {
        return messages.stream()
                .map(message -> message.getData().toString(StandardCharsets.UTF_8))
                .collect(Collectors.toList());
    }

    private PubSubSplitReaders() {}
}
