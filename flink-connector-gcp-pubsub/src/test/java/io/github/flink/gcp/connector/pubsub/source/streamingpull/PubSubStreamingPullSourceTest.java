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

package io.github.flink.gcp.connector.pubsub.source.streamingpull;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;

import io.github.flink.gcp.connector.pubsub.source.PubSubSource;
import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.serializer.PubSubDeserializationSchema;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the acknowledgement-extension headroom warning of {@link PubSubStreamingPullSource}.
 */
class PubSubStreamingPullSourceTest {

    @TempDir Path tempDir;

    @Test
    void loadsTheConfiguredKeyWhenTheReaderIsCreated() {
        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                sourceWithMissingKeyFile();

        assertThatThrownBy(() -> source.createReader(new FakeSourceReaderContext(null)))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Pub/Sub service-account key file.");
    }

    @Test
    void loadsTheConfiguredKeyWhenTheEnumeratorIsCreated() {
        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                sourceWithMissingKeyFile();

        assertThatThrownBy(() -> source.createEnumerator(new FakeSplitEnumeratorContext<>(1)))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Pub/Sub service-account key file.");
    }

    @Test
    void loadsTheConfiguredKeyWhenTheEnumeratorIsRestored() {
        Source<String, SubscriptionSplit, PubSubEnumeratorState> source =
                sourceWithMissingKeyFile();
        PubSubEnumeratorState checkpoint =
                new PubSubEnumeratorState(
                        List.of(SubscriptionDestination.of("test-project", "test-subscription")),
                        false);

        assertThatThrownBy(
                        () ->
                                source.restoreEnumerator(
                                        new FakeSplitEnumeratorContext<>(1), checkpoint))
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Pub/Sub service-account key file.");
    }

    @Test
    void saysNothingWhenTheCheckpointIntervalIsNotVisibleToTheReader() {
        // A reader is handed the TaskManager configuration, so an interval set with
        // env.enableCheckpointing(...) is simply absent here — it must not be read as a problem.
        assertThat(
                        PubSubStreamingPullSource.ackExtensionHeadroomWarning(
                                new Configuration(), PubSubSubscriberOptions.defaults()))
                .isNull();
    }

    @Test
    void saysNothingWhenTheCheckpointIntervalLeavesAmpleHeadroom() {
        assertThat(
                        PubSubStreamingPullSource.ackExtensionHeadroomWarning(
                                configurationWithInterval(Duration.ofMinutes(1)),
                                PubSubSubscriberOptions.defaults()))
                .isNull();
    }

    @Test
    void warnsWhenTwiceTheCheckpointIntervalExceedsTheAckExtensionBudget() {
        assertThat(
                        PubSubStreamingPullSource.ackExtensionHeadroomWarning(
                                configurationWithInterval(Duration.ofMinutes(45)),
                                PubSubSubscriberOptions.defaults()))
                .contains("PT45M")
                .contains("PT1H")
                .contains("maxAckExtensionPeriod");
    }

    @Test
    void aConfiguredAckExtensionPeriodMovesTheThreshold() {
        Configuration configuration = configurationWithInterval(Duration.ofMinutes(1));
        PubSubSubscriberOptions options =
                PubSubSubscriberOptions.builder()
                        .maxAckExtensionPeriod(Duration.ofSeconds(90))
                        .build();

        assertThat(PubSubStreamingPullSource.ackExtensionHeadroomWarning(configuration, options))
                .contains("PT1M30S");
    }

    /**
     * A zero extension period became reachable when the knob started forwarding the client
     * library's own value for "disable auto deadline extension" (ADR-0068). The headroom sentence
     * cannot describe it — there is no budget to have headroom under — and telling a user to raise
     * a knob they deliberately zeroed is advice to undo their own configuration, so this state gets
     * its consequence stated instead.
     */
    @Test
    void aDisabledAckExtensionSaysWhatItCostsRatherThanAskingForHeadroom() {
        String warning =
                PubSubStreamingPullSource.ackExtensionHeadroomWarning(
                        configurationWithInterval(Duration.ofMinutes(1)),
                        PubSubSubscriberOptions.builder()
                                .maxAckExtensionPeriod(Duration.ZERO)
                                .build());

        assertThat(warning)
                .contains("extension is disabled")
                .contains("PT1M")
                .contains("redeliver")
                .doesNotContain("headroom")
                .doesNotContain("raise");
    }

    @Test
    void aDisabledCheckpointIntervalDoesNotWarnAboutHeadroom() {
        // Checkpointing disabled cluster-wide is the watchdog's business, not this warning's.
        assertThat(
                        PubSubStreamingPullSource.ackExtensionHeadroomWarning(
                                configurationWithInterval(Duration.ZERO),
                                PubSubSubscriberOptions.defaults()))
                .isNull();
    }

    private static Configuration configurationWithInterval(Duration interval) {
        Configuration configuration = new Configuration();
        configuration.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, interval);
        return configuration;
    }

    private Source<String, SubscriptionSplit, PubSubEnumeratorState> sourceWithMissingKeyFile() {
        return PubSubSource.<String>builder()
                .subscription(SubscriptionDestination.of("test-project", "test-subscription"))
                .deserializationSchema(
                        PubSubDeserializationSchema.dataOnly(new SimpleStringSchema()))
                .serviceAccountKeyFile(tempDir.resolve("missing-key.json").toString())
                .build();
    }
}
