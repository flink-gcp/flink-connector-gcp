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

package io.github.flink.gcp.connector.bigtable.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BigtableSourceBuilder}. */
@Timeout(30)
class BigtableSourceBuilderTest {

    private static BigtableSourceBuilder<String> minimal() {
        return BigtableSource.<String>builder()
                .table(TestSources.TABLE)
                .deserializer(new TestSources.RowKeyDeserializer())
                // The builder creates this source's real clients, which would demand
                // application-default credentials on a machine that has them and fail in CI on one
                // that does not. The endpoint is never connected to.
                .emulatorEndpoint("localhost:1");
    }

    private static List<String> rangesOf(BigtableSourceConfig<?> config) {
        return config.getRanges().stream().map(RowRanges::format).collect(Collectors.toList());
    }

    @Test
    void requiresATableAndADeserializer() {
        assertThatThrownBy(
                        () ->
                                BigtableSource.<String>builder()
                                        .deserializer(new TestSources.RowKeyDeserializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("A table is required");
        assertThatThrownBy(() -> BigtableSource.<String>builder().table(TestSources.TABLE).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("A deserializer is required");
    }

    @Test
    void readsTheWholeTableWhenNoRangeIsConfigured() {
        assertThat(rangesOf(TestSources.config())).containsExactly("(*, *)");
    }

    @Test
    void isBounded() {
        assertThat(TestSources.source(builder -> builder).getBoundedness())
                .isEqualTo(Boundedness.BOUNDED);
    }

    @Test
    void takesItsProducedTypeFromTheDeserializer() {
        assertThat(TestSources.source(builder -> builder).getProducedType().getTypeClass())
                .isEqualTo(String.class);
    }

    @Test
    void convertsAPrefixToTheRangeItDescribes() {
        assertThat(rangesOf(TestSources.config(builder -> builder.prefix("user"))))
                .containsExactly("[user, uses)");
    }

    @Test
    void convertsAPrefixWithNoSuccessorToARangeRunningToTheEndOfTheTable() {
        // An all-0xff prefix has no next key, and hand-rolled prefix arithmetic gets this wrong.
        ByteString allOnes = ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xFF});

        BigtableSourceConfig<String> config =
                TestSources.config(builder -> builder.prefix(allOnes));

        assertThat(config.getRanges().get(0).getEndBound()).isEqualTo(BoundType.UNBOUNDED);
        assertThat(config.getRanges().get(0).getStart()).isEqualTo(allOnes);
    }

    @Test
    void readsTheWholeTableForAnEmptyPrefix() {
        assertThat(rangesOf(TestSources.config(builder -> builder.prefix(""))))
                .containsExactly("(*, *)");
    }

    @Test
    void mergesOverlappingRanges() {
        // Two nested prefixes are the accident this protects against: without merging, the rows
        // they share land in two splits, which two subtasks read, and a successful run emits them
        // twice.
        assertThat(rangesOf(TestSources.config(builder -> builder.prefix("user").prefix("user1"))))
                .containsExactly("[user, uses)");
    }

    @Test
    void keepsDisjointRangesApartAndInOrder() {
        assertThat(
                        rangesOf(
                                TestSources.config(
                                        builder -> builder.rowRange("p", "q").rowRange("a", "b"))))
                .containsExactly("[a, b)", "[p, q)");
    }

    @Test
    void rejectsARangeThatHoldsNoRowKey() {
        // A range reading nothing under a successful job looks exactly like a job with nothing to
        // read, so it is refused where it was typed.
        assertThatThrownBy(() -> minimal().rowRange("m", "m").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("[m, m)")
                .hasMessageContaining("would read nothing");
        assertThatThrownBy(() -> minimal().rowRange("z", "a").build())
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                minimal()
                                        .rowRange(
                                                ByteStringRange.unbounded()
                                                        .startOpen("m")
                                                        .endClosed("m"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsAnEmptyBoundKeyRatherThanWideningTheRange() {
        // The client library turns an empty key into an unbounded side, so rowRange("", "") would
        // otherwise become a scan of the whole table — and no emptiness check can object, because
        // an unbounded side is never empty. The shape arrives from configuration that defaulted.
        assertThatThrownBy(() -> minimal().rowRange("", "").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("startClosed must not be empty");
        assertThatThrownBy(() -> minimal().rowRange("z", "").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endOpen must not be empty");
        assertThatThrownBy(
                        () ->
                                minimal()
                                        .rowRange(ByteString.EMPTY, ByteString.copyFromUtf8("z"))
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void doesNotShareRangesWithTheCallerThatSuppliedThem() {
        // Vendor ranges are mutable and their mutators return the receiver, so a shared reference
        // would let a caller rewrite a plan after the source was built.
        ByteStringRange supplied = ByteStringRange.unbounded().startClosed("a").endOpen("m");
        BigtableSourceConfig<String> config =
                TestSources.config(builder -> builder.rowRange(supplied));

        supplied.startClosed("z").endUnbounded();

        assertThat(rangesOf(config)).containsExactly("[a, m)");
    }

    @Test
    void doesNotHandOutRangesACallerCouldEdit() {
        BigtableSourceConfig<String> config =
                TestSources.config(builder -> builder.rowRange("a", "m"));

        config.getRanges().get(0).startClosed("z");

        assertThat(rangesOf(config)).containsExactly("[a, m)");
    }

    @Test
    void carriesTheFilterAndTheApplicationProfile() {
        Filters.Filter filter = Filters.FILTERS.family().exactMatch("cf");

        BigtableSourceConfig<String> config =
                TestSources.config(builder -> builder.filter(filter).appProfileId("batch-profile"));

        assertThat(config.getFilter()).isEqualTo(filter);
        assertThat(config.getAppProfileId()).isEqualTo("batch-profile");
        assertThat(config.getServiceAccountKeyFile()).isNull();
    }

    @Test
    void serviceAccountKeyFilePropagatesWithoutBeingParsedAtBuildTime() {
        BigtableSourceConfig<String> config =
                ((io.github.flink.gcp.connector.bigtable.source.readrows.BigtableReadRowsSource<
                                        String>)
                                BigtableSource.<String>builder()
                                        .table(TestSources.TABLE)
                                        .deserializer(new TestSources.RowKeyDeserializer())
                                        .serviceAccountKeyFile("/var/run/secrets/bigtable.json")
                                        .build())
                        .getConfig();

        assertThat(config.getServiceAccountKeyFile()).isEqualTo("/var/run/secrets/bigtable.json");
    }

    @Test
    void rejectsNullOrBlankServiceAccountKeyFile() {
        assertThatThrownBy(() -> BigtableSource.<String>builder().serviceAccountKeyFile(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("serviceAccountKeyFile must not be null");
        assertThatThrownBy(() -> BigtableSource.<String>builder().serviceAccountKeyFile(" \t"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("serviceAccountKeyFile must not be blank");
    }

    @Test
    void rejectsAServiceAccountKeyFileAlongsideAnEmulatorInEitherOrder() {
        assertThatThrownBy(() -> minimal().serviceAccountKeyFile("key.json").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)");
        assertThatThrownBy(
                        () ->
                                BigtableSource.<String>builder()
                                        .table(TestSources.TABLE)
                                        .deserializer(new TestSources.RowKeyDeserializer())
                                        .serviceAccountKeyFile("key.json")
                                        .emulatorEndpoint("localhost:1")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("emulatorEndpoint(...)");
    }

    @Test
    void rejectsABlankApplicationProfile() {
        assertThatThrownBy(() -> minimal().appProfileId("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("appProfileId must not be blank");
        // U+2028 is the value that tells the two idioms apart: Character.isWhitespace calls it
        // whitespace, and String.trim() leaves it alone because it sits above U+0020. Only this
        // assertion fails if appProfileId returns to trim().isEmpty(); the ASCII one above passes
        // either way. serviceAccountKeyFile, checked here beside it, has always rejected it.
        assertThatThrownBy(() -> minimal().appProfileId("\u2028"))
                .as("U+2028 is blank to isBlank() but survives trim()")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("appProfileId must not be blank");
        assertThatThrownBy(() -> BigtableSource.<String>builder().serviceAccountKeyFile("\u2028"))
                .as("the sibling check this one was aligned with")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("serviceAccountKeyFile must not be blank");
    }

    @Test
    void rejectsAMalformedEmulatorEndpointWhereItIsTyped() {
        assertThatThrownBy(() -> BigtableSource.<String>builder().emulatorEndpoint("localhost"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulatorEndpoint must be host:port, was 'localhost'");
    }

    @Test
    void rejectsAFilterTooLargeForTheService() {
        // The client's own size precondition, reached at build time through a throwaway query.
        // Without it the rejection arrives on a TaskManager, once per subtask, as a restart loop.
        StringBuilder huge = new StringBuilder();
        for (int i = 0; i < 30_000; i++) {
            huge.append('x');
        }
        Filters.Filter oversized = Filters.FILTERS.qualifier().regex(huge.toString());

        assertThatThrownBy(() -> minimal().filter(oversized).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("filter size can't be more than 20KB");
    }

    @Test
    void takesTheLastFilterSet() {
        Filters.Filter last = Filters.FILTERS.family().exactMatch("second");

        BigtableSourceConfig<String> config =
                TestSources.config(
                        builder ->
                                builder.filter(Filters.FILTERS.family().exactMatch("first"))
                                        .filter(last));

        assertThat(config.getFilter()).isEqualTo(last);
    }

    @Test
    void rejectsNullArguments() {
        assertThatThrownBy(() -> BigtableSource.<String>builder().table(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> BigtableSource.<String>builder().deserializer(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> BigtableSource.<String>builder().rowRange((ByteStringRange) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> BigtableSource.<String>builder().prefix((String) null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> BigtableSource.<String>builder().filter(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> BigtableSource.<String>builder().appProfileId(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void travelsInTheJobGraph() throws Exception {
        Source<String, ?, ?> source =
                minimal()
                        .prefix("user")
                        .filter(Filters.FILTERS.family().exactMatch("cf"))
                        .appProfileId("batch-profile")
                        .build();

        Source<String, ?, ?> back =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(source), getClass().getClassLoader());

        assertThat(back.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
    }

    @Test
    void readsAnyTableItIsPointedAt() {
        TableDestination other = TableDestination.of("other-p", "other-i", "events");

        assertThat(TestSources.config(builder -> builder.table(other)).getTable()).isEqualTo(other);
    }
}
