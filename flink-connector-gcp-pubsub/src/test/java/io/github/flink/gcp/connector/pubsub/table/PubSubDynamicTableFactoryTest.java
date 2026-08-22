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

package io.github.flink.gcp.connector.pubsub.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;

import io.github.flink.gcp.connector.pubsub.sink.PubSubPublisherSink;
import io.github.flink.gcp.connector.pubsub.source.PubSubSourceConfig;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.PubSubStreamingPullSource;
import io.github.flink.gcp.connector.pubsub.table.sink.PubSubDynamicSink;
import io.github.flink.gcp.connector.pubsub.table.source.PubSubDynamicSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PubSubDynamicTableFactory}. */
class PubSubDynamicTableFactoryTest {

    private static final String SERVICE_ACCOUNT_KEY_FILE =
            "/var/run/secrets/gcp/service-account.json";

    private static final ResolvedSchema SCHEMA =
            ResolvedSchema.of(
                    Column.physical("id", DataTypes.STRING()),
                    Column.physical("amount", DataTypes.INT()));

    private static Map<String, String> minimalSinkOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", PubSubDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("topic", "my-topic");
        options.put("format", "json");
        return options;
    }

    @Test
    void buildsASinkFromTheMinimalOptionSet() {
        DynamicTableSink sink = FactoryMocks.createTableSink(SCHEMA, minimalSinkOptions());

        assertThat(sink).isInstanceOf(PubSubDynamicSink.class);
        assertThat(sink.asSummaryString()).isEqualTo("Pub/Sub table sink");
    }

    @ParameterizedTest(name = "format={0}")
    @ValueSource(strings = {"json", "csv"})
    void discoversAnyEncodingFormat(String format) {
        Map<String, String> options = minimalSinkOptions();
        options.put("format", format);

        assertThat(FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(PubSubDynamicSink.class);
    }

    @Test
    void rejectsASinkWithoutATopic() {
        Map<String, String> options = minimalSinkOptions();
        options.remove("topic");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Option 'topic' is required to write to a 'pubsub' table.");
    }

    @Test
    void rejectsAnUnknownOption() {
        Map<String, String> options = minimalSinkOptions();
        options.put("sink.batching.element-count-thresholds", "10");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("sink.batching.element-count-thresholds");
    }

    @Test
    void passesTheConfiguredSinkParallelismToTheProvider() {
        Map<String, String> options = minimalSinkOptions();
        options.put("sink.parallelism", "3");

        DynamicTableSink.SinkRuntimeProvider provider =
                FactoryMocks.createTableSink(SCHEMA, options)
                        .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(provider).isInstanceOf(SinkV2Provider.class);
        assertThat(((SinkV2Provider) provider).getParallelism()).contains(3);
    }

    @Test
    void leavesTheSinkParallelismUnsetWhenTheOptionIsAbsent() {
        DynamicTableSink.SinkRuntimeProvider provider =
                FactoryMocks.createTableSink(SCHEMA, minimalSinkOptions())
                        .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

        assertThat(((SinkV2Provider) provider).getParallelism()).isEmpty();
    }

    @Test
    void rejectsATopicGivenAsAResourcePath() {
        Map<String, String> options = minimalSinkOptions();
        options.put("topic", "projects/p/topics/t");

        // The connector's own validation is the one that speaks: a TopicDestination component is a
        // bare name, never a resource path.
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must not contain '/'");
    }

    /**
     * What a SQL user actually meets, and the reason the mapper restates the builder's check in DDL
     * keys. {@code FactoryUtil.createDynamicTableSink} wraps anything the factory throws in a
     * {@code ValidationException} whose own message says only "Unable to create a sink for writing
     * table ...", so the actionable sentence arrives in the cause — hence {@code
     * hasStackTraceContaining}, as the two tests above already do.
     */
    @Test
    void rejectsABoundedRetryBudgetBesideMessageOrdering() {
        Map<String, String> options = minimalSinkOptions();
        options.put("sink.retry.total-timeout", "5 min");
        options.put("sink.message-ordering.enabled", "true");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("cannot be combined with")
                // This is the discriminating one, and the key names are deliberately not: it says
                // 'sink.retry.*' where the builder's message says "retry knobs", so it holds only
                // if the mapper's DDL-worded check is what fired. Asserting the key names instead
                // proves nothing — FactoryUtil's own message dumps every WITH option, so
                // "'sink.retry.total-timeout' appears somewhere" is satisfied by the dump even
                // with the mapper's guard deleted. Measured, by deleting it.
                .hasStackTraceContaining("The other six 'sink.retry.*' options are unaffected");
    }

    @Test
    void rejectsAnEmptyTopic() {
        Map<String, String> options = minimalSinkOptions();
        options.put("topic", "");

        // An empty value is *present*, so the factory's own "required to write" check does not
        // fire; the destination's precondition is what catches it.
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must not be blank");
    }

    @Test
    void rejectsASinkWithoutAProject() {
        Map<String, String> options = minimalSinkOptions();
        options.remove("project");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("project");
    }

    @Test
    void rejectsASinkWithoutAFormat() {
        Map<String, String> options = minimalSinkOptions();
        options.remove("format");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("format");
    }

    @Test
    void rejectsAnUnknownFormat() {
        Map<String, String> options = minimalSinkOptions();
        options.put("format", "no-such-format");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("no-such-format");
    }

    @Test
    void rejectsAnUnparseableCreateDisposition() {
        Map<String, String> options = minimalSinkOptions();
        options.put("sink.create-disposition", "create_if_needed");

        // The DDL spelling is hyphenated; the constant name is not accepted, and the message lists
        // what is.
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("sink.create-disposition");
    }

    // ------------------------------------------------------------------------
    //  Source
    // ------------------------------------------------------------------------

    private static Map<String, String> minimalSourceOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", PubSubDynamicTableFactory.IDENTIFIER);
        options.put("project", "my-project");
        options.put("subscription", "my-sub");
        options.put("format", "json");
        return options;
    }

    @Test
    void buildsASourceFromTheMinimalOptionSet() {
        DynamicTableSource source = FactoryMocks.createTableSource(SCHEMA, minimalSourceOptions());

        assertThat(source).isInstanceOf(PubSubDynamicSource.class);
        assertThat(source.asSummaryString()).isEqualTo("Pub/Sub table source");
    }

    @Test
    void rejectsASourceWithoutASubscription() {
        Map<String, String> options = minimalSourceOptions();
        options.remove("subscription");

        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Option 'subscription' is required to read from a 'pubsub' table.");
    }

    @Test
    void splitsTheSubscriptionListOnSemicolons() {
        Map<String, String> options = minimalSourceOptions();
        options.put("subscription", "sub-a;sub-b");

        // Two factory-built sources can never be compared: each discovers its own format instance
        // and formats compare by reference. So the list is read back through behaviour instead —
        // both elements reaching SubscriptionDestination.of is what the neighbouring tests prove,
        // and this one proves the happy path builds at all rather than treating the whole string
        // as one subscription name (which would fail on the ';').
        assertThat(
                        ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(SourceProvider.class);
    }

    @Test
    void rejectsASubscriptionGivenAsAResourcePath() {
        // The mistake the docs warn about: the `subscription` *column* is a resource name, so it is
        // natural to write one here too. The option is a bare id.
        Map<String, String> options = minimalSourceOptions();
        options.put("subscription", "projects/p/subscriptions/s");

        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("must not contain '/'");
    }

    @Test
    void rejectsAnEmptySubscriptionList() {
        Map<String, String> options = minimalSourceOptions();
        options.put("subscription", "");

        // An empty value parses to a present-but-empty list, so the factory's own "required to
        // read" check does not fire; the builder is what catches it.
        assertThatThrownBy(
                        () ->
                                ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options))
                                        .getScanRuntimeProvider(
                                                ScanRuntimeProviderContext.INSTANCE))
                .hasStackTraceContaining("subscription");
    }

    @Test
    void letsTheSourceBuilderRejectAnImpossibleCombination() {
        Map<String, String> options = minimalSourceOptions();
        options.put("scan.ordering-mode", "per-key");
        options.put("scan.parallel-pull-count", "4");

        // Not re-implemented in the factory: the builder already refuses this with a message that
        // names the setter, and duplicating it here would be a second place to keep correct.
        assertThatThrownBy(
                        () ->
                                ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options))
                                        .getScanRuntimeProvider(
                                                ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(IllegalStateException.class)
                .hasStackTraceContaining("orderingMode(PER_KEY)");
    }

    @Test
    void rejectsADuplicatedSubscription() {
        Map<String, String> options = minimalSourceOptions();
        options.put("subscription", "sub-a;sub-a");

        assertThatThrownBy(
                        () ->
                                ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options))
                                        .getScanRuntimeProvider(
                                                ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(IllegalStateException.class)
                // The builder's own sentence, not the value echoed back from the configuration.
                .hasStackTraceContaining("distinct");
    }

    @Test
    void oneOptionMapServesBothDirections() {
        Map<String, String> options = minimalSinkOptions();
        options.putAll(minimalSourceOptions());

        // The common shape: one CREATE TABLE both INSERTed into and SELECTed from. Neither
        // direction's options may be rejected as unknown by the other.
        assertThat(FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(PubSubDynamicSink.class);
        assertThat(FactoryMocks.createTableSource(SCHEMA, options))
                .isInstanceOf(PubSubDynamicSource.class);
    }

    @Test
    void mapsTheServiceAccountKeyFileIntoBothBuilders() {
        Map<String, String> sinkOptions = minimalSinkOptions();
        sinkOptions.put("service-account-key-file", SERVICE_ACCOUNT_KEY_FILE);
        SinkV2Provider sinkProvider =
                (SinkV2Provider)
                        FactoryMocks.createTableSink(SCHEMA, sinkOptions)
                                .getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
        PubSubPublisherSink<?> sink = (PubSubPublisherSink<?>) sinkProvider.createSink();

        Map<String, String> sourceOptions = minimalSourceOptions();
        sourceOptions.put("service-account-key-file", SERVICE_ACCOUNT_KEY_FILE);
        SourceProvider sourceProvider =
                (SourceProvider)
                        ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, sourceOptions))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        PubSubSourceConfig<?> sourceConfig =
                ((PubSubStreamingPullSource<?>) sourceProvider.createSource()).getConfig();

        assertThat(sink.getConfig().getServiceAccountKeyFile()).isEqualTo(SERVICE_ACCOUNT_KEY_FILE);
        assertThat(sourceConfig.getServiceAccountKeyFile()).isEqualTo(SERVICE_ACCOUNT_KEY_FILE);
    }

    @Test
    void rejectsAServiceAccountKeyFileAlongsideAnEmulatorForBothDirections() {
        Map<String, String> sinkOptions = minimalSinkOptions();
        sinkOptions.put("service-account-key-file", SERVICE_ACCOUNT_KEY_FILE);
        sinkOptions.put("emulator-endpoint", "localhost:8085");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, sinkOptions))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("cannot be combined: an emulator uses a plaintext")
                .hasStackTraceContaining("service-account-key-file")
                .hasStackTraceContaining("emulator-endpoint");

        Map<String, String> sourceOptions = minimalSourceOptions();
        sourceOptions.put("service-account-key-file", SERVICE_ACCOUNT_KEY_FILE);
        sourceOptions.put("emulator-endpoint", "localhost:8085");
        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, sourceOptions))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("cannot be combined: an emulator uses a plaintext")
                .hasStackTraceContaining("service-account-key-file")
                .hasStackTraceContaining("emulator-endpoint");
    }

    /**
     * Issue #1019: the rejection names {@code emulator-endpoint}, the key the DDL carried, rather
     * than the {@code emulatorEndpoint(...)} setter the value used to reach on its way to a
     * publisher or subscriber.
     *
     * <p>Asserted on the root cause. {@code FactoryUtil} dumps every {@code WITH} option into its
     * own message, so a needle of just the key would pass with the parse deleted; the root cause is
     * the {@code IllegalArgumentException} the parse throws and carries nothing else. The needle
     * also discriminates the fix, since {@code emulator-endpoint must be} is not a substring of
     * {@code emulatorEndpoint must be}.
     *
     * <p>Two values, not a catalogue. {@code "localhost"} exercises the shape, and {@code ""} the
     * one thing that is this layer's rather than the parser's: whether an option written {@code ''}
     * arrives as present-and-empty rather than absent, so the check sees it at all. The rejection
     * set itself belongs to {@code EmulatorEndpointTest}.
     */
    @Test
    void rejectsAMalformedEmulatorEndpointInBothDirections() {
        for (String malformed : new String[] {"localhost", ""}) {
            String message = "emulator-endpoint must be host:port, was '" + malformed + "'";

            Map<String, String> sinkOptions = minimalSinkOptions();
            sinkOptions.put("emulator-endpoint", malformed);
            assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, sinkOptions))
                    .as("sink, '%s'", malformed)
                    .isInstanceOf(ValidationException.class)
                    .rootCause()
                    .hasMessage(message);

            Map<String, String> sourceOptions = minimalSourceOptions();
            sourceOptions.put("emulator-endpoint", malformed);
            assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, sourceOptions))
                    .as("source, '%s'", malformed)
                    .isInstanceOf(ValidationException.class)
                    .rootCause()
                    .hasMessage(message);
        }
    }

    /**
     * Pins the endpoint parse behind every check this factory makes that refuses an option outright
     * — a DDL told to remove an option is not helped by an answer about that option's shape.
     *
     * <p>Asserted on the root cause and paired with the negative: with the parse moved above {@code
     * validateCredentialsMode} or {@code validateAutoCreateTopicsSyntax} the root cause becomes the
     * {@code IllegalArgumentException}, whose message these phrases do not appear in.
     *
     * <p>Green on {@code origin/main} by construction. It guards the ordering, not the fix.
     */
    @Test
    void refusesAnOptionOutrightBeforeReportingTheEndpointShape() {
        Map<String, String> credentials = minimalSinkOptions();
        credentials.put("service-account-key-file", SERVICE_ACCOUNT_KEY_FILE);
        credentials.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, credentials))
                .as("an emulator endpoint beside a key file")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("cannot be combined: an emulator uses a plaintext")
                .hasMessageNotContaining("must be host:port");

        Map<String, String> bothSyntaxes = minimalSourceOptions();
        bothSyntaxes.put("scan.auto-create.topics", "my-sub:my-topic");
        bothSyntaxes.put("scan.auto-create.topics.my-sub", "my-topic");
        bothSyntaxes.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, bothSyntaxes))
                .as("both map syntaxes on a source")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("must use either the packed map syntax")
                .hasMessageNotContaining("must be host:port");

        Map<String, String> noTopic = minimalSinkOptions();
        noTopic.remove("topic");
        noTopic.put("emulator-endpoint", "localhost");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, noTopic))
                .as("a sink that has not said where it points")
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessageContaining("is required to write to a 'pubsub' table")
                .hasMessageNotContaining("must be host:port");
    }

    /**
     * Issue #1027: a configured name is refused under the option key the DDL carried, not under the
     * {@code kmsKeyName(...)} setter a SQL caller never wrote. Same defect #1019 fixed for {@code
     * emulator-endpoint}.
     *
     * <p>Asserted on the root cause, because {@code FactoryUtil} dumps every {@code WITH} option
     * into its own message and a needle of just the key would pass with the check deleted.
     *
     * <p>The check is reached through {@code TopicCreateOptionsMapper}, which runs only when the
     * sink is assembled, so the create disposition has to allow creation for the arm to fire at
     * all.
     */
    @Test
    void refusesAKmsKeyNameUnderTheOptionKeyTheDdlCarried() {
        Map<String, String> options = minimalSinkOptions();
        options.put("sink.create-disposition", "create-if-needed");
        options.put("sink.auto-create.kms-key-name", "  ");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessage("sink.auto-create.kms-key-name must not be blank");

        // The neighbouring list option, found by an independent review of this change. A list
        // names the entry rather than the option alone: the option is not blank, one entry is.
        Map<String, String> regions = minimalSinkOptions();
        regions.put("sink.create-disposition", "create-if-needed");
        // A blank entry written as a space rather than as an empty one, because checkstyle's
        // "use one semicolon" rule reads a doubled separator in a literal as a stray statement.
        regions.put(
                "sink.auto-create.storage-policy.allowed-regions", "us-central1; ;europe-west1");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, regions))
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessage(
                        "an entry of sink.auto-create.storage-policy.allowed-regions must not be"
                                + " blank");

        // A value written blank parses to an empty list, so it never reaches the per-entry check
        // and would otherwise be answered by the builder's own "must not be empty".
        Map<String, String> noRegions = minimalSinkOptions();
        noRegions.put("sink.create-disposition", "create-if-needed");
        noRegions.put("sink.auto-create.storage-policy.allowed-regions", "  ");
        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, noRegions))
                .isInstanceOf(ValidationException.class)
                .rootCause()
                .hasMessage("sink.auto-create.storage-policy.allowed-regions must not be empty");
    }

    @Test
    void rejectsABlankServiceAccountKeyFile() {
        Map<String, String> options = minimalSinkOptions();
        options.put("service-account-key-file", "  ");

        assertThatThrownBy(() -> FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Option 'service-account-key-file' must not be blank.");
    }

    @Test
    void passesTheConfiguredSourceParallelismToTheProvider() {
        Map<String, String> options = minimalSourceOptions();
        options.put("scan.parallelism", "5");

        ScanTableSource.ScanRuntimeProvider provider =
                ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options))
                        .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider).isInstanceOf(SourceProvider.class);
        assertThat(((SourceProvider) provider).getParallelism()).contains(5);
    }

    @Test
    void buildsASourceFromAFullAutoCreationBlock() {
        Map<String, String> options = minimalSourceOptions();
        options.put("scan.auto-create.topics.my-sub", "my-topic");
        options.put("scan.auto-create.ack-deadline", "60 s");
        options.put("scan.auto-create.message-ordering.enabled", "true");
        options.put("scan.auto-create.message-retention", "3 d");
        options.put("scan.auto-create.retain-acked-messages", "true");
        options.put("scan.auto-create.never-expire", "true");
        options.put("scan.auto-create.dead-letter.topic", "my-dlq");
        options.put("scan.auto-create.dead-letter.max-delivery-attempts", "5");
        options.put("scan.auto-create.filter", "attributes.kind = \"order\"");

        // Every key parses and is accepted rather than rejected as unknown, and the resulting
        // source builds. What each one becomes is SubscriptionCreateOptionsMapperTest's job.
        assertThat(
                        ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(SourceProvider.class);
    }

    @Test
    void buildsASourceFromAStartPosition() {
        Map<String, String> options = minimalSourceOptions();
        options.put("scan.startup.mode", "timestamp");
        options.put("scan.startup.timestamp-millis", "1735689600000");

        assertThat(
                        ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(SourceProvider.class);
    }

    @Test
    void buildsASourceWithDistinctTopicsForSeveralSubscriptions() {
        Map<String, String> options = minimalSourceOptions();
        options.put("subscription", "orders;returns");
        options.put("scan.auto-create.topics.orders", "orders-topic");
        options.put("scan.auto-create.topics.returns", "returns-topic");

        assertThat(
                        ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(SourceProvider.class);
    }

    @Test
    void buildsASourceWithSeveralPackedTopicMappings() {
        Map<String, String> options = minimalSourceOptions();
        options.put("subscription", "orders;returns");
        options.put("scan.auto-create.topics", "orders:orders-topic,returns:returns-topic");

        assertThat(
                        ((ScanTableSource) FactoryMocks.createTableSource(SCHEMA, options))
                                .getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(SourceProvider.class);
    }

    @Test
    void rejectsMixingPackedAndPrefixedTopicMappings() {
        Map<String, String> options = minimalSourceOptions();
        options.put("scan.auto-create.topics", "my-sub:old-topic");
        options.put("scan.auto-create.topics.my-sub", "new-topic");

        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("either the packed map syntax or prefixed map entries")
                .hasStackTraceContaining("not both");
    }

    @Test
    void rejectsAutoCreationWhenATopicMappingIsMissing() {
        Map<String, String> options = minimalSourceOptions();
        options.put("subscription", "orders;returns");
        options.put("scan.auto-create.topics.orders", "orders-topic");

        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Missing keys: [returns]");
    }

    @Test
    void rejectsTheRemovedSingularAutoCreateTopicOption() {
        Map<String, String> options = minimalSourceOptions();
        options.put("scan.auto-create.topic", "my-topic");

        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("Unsupported options")
                .hasStackTraceContaining("scan.auto-create.topic");
    }

    @Test
    void rejectsAStartupTimestampWithNoMode() {
        Map<String, String> options = minimalSourceOptions();
        options.put("scan.startup.timestamp-millis", "1735689600000");

        assertThatThrownBy(() -> FactoryMocks.createTableSource(SCHEMA, options))
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining("scan.startup.mode");
    }

    @Test
    void theAutoCreationBlockDoesNotDisturbASinkThatSharesTheOptionMap() {
        // The sink direction never reads scan.* — but the options are declared on one factory, so
        // they must still be accepted as known keys rather than failing an INSERT into a table
        // whose DDL configures its scan half.
        Map<String, String> options = minimalSinkOptions();
        options.putAll(minimalSourceOptions());
        options.put("scan.auto-create.topics.my-sub", "my-topic");
        options.put("scan.startup.mode", "earliest-retained");

        assertThat(FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(PubSubDynamicSink.class);
    }

    @Test
    void acceptsTheHyphenatedCreateDisposition() {
        Map<String, String> options = minimalSinkOptions();
        options.put("sink.create-disposition", "create-never");

        // That the value actually reaches the writer is asserted end to end in
        // PubSubTableSinkITCase; here it only has to parse, since the sink exposes no getter and
        // comparing two factory-built sinks would compare two distinct format instances.
        assertThat(FactoryMocks.createTableSink(SCHEMA, options))
                .isInstanceOf(PubSubDynamicSink.class);
    }
}
