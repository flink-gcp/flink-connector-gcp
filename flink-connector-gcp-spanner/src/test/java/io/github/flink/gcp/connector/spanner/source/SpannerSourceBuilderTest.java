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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.batch.PartitionSplit;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchEnumeratorState;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchReadSource;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.BatchClientPartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.reader.BatchClientStructStreamOpener;
import io.github.flink.gcp.connector.spanner.source.batch.reader.SpannerSplitReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerSourceBuilder}. */
class SpannerSourceBuilderTest {

    private static final SpannerDatabase DATABASE =
            SpannerDatabase.of("my-project", "my-instance", "my-db");
    private static final SpannerReadOperation OPERATION =
            SpannerReadOperation.query(Statement.of("SELECT id FROM singers"));

    @Test
    void everyOptionReachesTheConfiguration() {
        SpannerSourceConfig<Long> config =
                configOf(
                        builder()
                                .timestampBound(
                                        TimestampBound.ofExactStaleness(5, TimeUnit.SECONDS))
                                .maxPartitions(12)
                                .partitionSizeBytes(4096)
                                .dataBoostEnabled(true));

        assertThat(config.getDatabase()).isEqualTo(DATABASE);
        assertThat(config.getReadOperation()).isEqualTo(OPERATION);
        assertThat(config.getTimestampBound().getMode())
                .isEqualTo(TimestampBound.Mode.EXACT_STALENESS);
        assertThat(config.getPartitionOptions().getMaxPartitions()).isEqualTo(12);
        assertThat(config.getPartitionOptions().getPartitionSizeBytes()).isEqualTo(4096);
        assertThat(config.isDataBoostEnabled()).isTrue();
        assertThat(config.getMaxRecordsPerFetch())
                .isEqualTo(SpannerSplitReader.DEFAULT_MAX_ROWS_PER_FETCH);
    }

    @Test
    void theDefaultsAreAStrongReadWithNoHintsAndNoDataBoost() {
        SpannerSourceConfig<Long> config = configOf(builder());

        assertThat(config.getTimestampBound().getMode()).isEqualTo(TimestampBound.Mode.STRONG);
        assertThat(config.isDataBoostEnabled()).isFalse();
        // Zero on both fields is the wire form for "no preference"; the connector never sends a
        // hint the user did not ask for.
        assertThat(config.getPartitionOptions().getMaxPartitions()).isZero();
        assertThat(config.getPartitionOptions().getPartitionSizeBytes()).isZero();
    }

    @Test
    void oneHintOnItsOwnLeavesTheOtherUnset() {
        SpannerSourceConfig<Long> config = configOf(builder().maxPartitions(7));

        assertThat(config.getPartitionOptions().getMaxPartitions()).isEqualTo(7);
        assertThat(config.getPartitionOptions().getPartitionSizeBytes()).isZero();
    }

    @Test
    void theRealSeamsAreUsedWhenNoneIsInjected() {
        // The production path, and the only test that asserts it: everything else here injects a
        // scripted seam, which would hide a builder that stopped wiring the real one.
        SpannerSourceConfig<Long> config = configOf(builder());

        assertThat(config.getPlanner()).isInstanceOf(BatchClientPartitionPlanner.class);
        assertThat(config.getOpener()).isInstanceOf(BatchClientStructStreamOpener.class);
    }

    @Test
    void theSourceIsBoundedAndDeclaresTheDeserializersType() {
        Source<Long, PartitionSplit, SpannerBatchEnumeratorState> source = builder().build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(((SpannerBatchReadSource<Long>) source).getProducedType())
                .isEqualTo(TypeInformation.of(Long.class));
    }

    @Test
    void aMissingRequiredOptionNamesItself() {
        assertThatThrownBy(() -> SpannerSource.<Long>builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("database(...)");
        assertThatThrownBy(() -> SpannerSource.<Long>builder().database(DATABASE).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("readOperation(...)");
        assertThatThrownBy(
                        () ->
                                SpannerSource.<Long>builder()
                                        .database(DATABASE)
                                        .readOperation(OPERATION)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("deserializer(...)");
    }

    static Stream<TimestampBound> boundsABatchReadCannotTake() {
        return Stream.of(
                TimestampBound.ofMaxStaleness(1, TimeUnit.SECONDS),
                TimestampBound.ofMinReadTimestamp(Timestamp.ofTimeMicroseconds(1_000L)));
    }

    @ParameterizedTest
    @MethodSource("boundsABatchReadCannotTake")
    void aSingleUseOnlyBoundIsRejectedAtTheSetter(TimestampBound bound) {
        // The client refuses these too, but only when the enumerator opens the transaction — on a
        // JobManager, naming a transaction the user never wrote. Refusing here names the knob.
        assertThatThrownBy(() -> builder().timestampBound(bound))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(bound.getMode().toString())
                .hasMessageContaining("single-use");
    }

    @Test
    void theThreeBoundsABatchReadCanTakeAreAccepted() {
        // The other arm of the check above: without this, a rejection widened to every mode would
        // pass the test that only asserts the refusals.
        assertThat(configOf(builder().timestampBound(TimestampBound.strong())).getTimestampBound())
                .isEqualTo(TimestampBound.strong());
        assertThat(
                        configOf(
                                        builder()
                                                .timestampBound(
                                                        TimestampBound.ofReadTimestamp(
                                                                Timestamp.ofTimeMicroseconds(1))))
                                .getTimestampBound()
                                .getMode())
                .isEqualTo(TimestampBound.Mode.READ_TIMESTAMP);
        assertThat(
                        configOf(
                                        builder()
                                                .timestampBound(
                                                        TimestampBound.ofExactStaleness(
                                                                1, TimeUnit.SECONDS)))
                                .getTimestampBound()
                                .getMode())
                .isEqualTo(TimestampBound.Mode.EXACT_STALENESS);
    }

    @Test
    void aNonPositiveHintIsRejected() {
        assertThatThrownBy(() -> builder().maxPartitions(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxPartitions must be positive");
        assertThatThrownBy(() -> builder().partitionSizeBytes(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partitionSizeBytes must be positive");
    }

    @Test
    void aMalformedEmulatorEndpointIsRejected() {
        assertThatThrownBy(() -> builder().emulatorEndpoint("localhost"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nullsAreRejectedAtTheSetterThatTookThem() {
        assertThatThrownBy(() -> SpannerSource.<Long>builder().database(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SpannerSource.<Long>builder().readOperation(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SpannerSource.<Long>builder().deserializer(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> SpannerSource.<Long>builder().timestampBound(null))
                .isInstanceOf(NullPointerException.class);
    }

    private static SpannerSourceBuilder<Long> builder() {
        return SpannerSource.<Long>builder()
                .database(DATABASE)
                .readOperation(OPERATION)
                .deserializer(new TestSources.IdDeserializer());
    }

    @SuppressWarnings("unchecked")
    private static SpannerSourceConfig<Long> configOf(SpannerSourceBuilder<Long> builder) {
        return ((SpannerBatchReadSource<Long>) builder.build()).getConfig();
    }
}
