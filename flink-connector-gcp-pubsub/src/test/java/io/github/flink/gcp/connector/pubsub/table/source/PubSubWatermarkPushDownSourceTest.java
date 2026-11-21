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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestFormatFactory.DecodingFormatMock;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;

import io.github.flink.gcp.connector.pubsub.source.PubSubSubscriberOptions;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link PubSubWatermarkPushDownSource} on its own; {@code
 * PubSubWatermarkPushDownPlanTest} drives it through the planner.
 */
class PubSubWatermarkPushDownSourceTest {

    private static final DataType PHYSICAL_DATA_TYPE =
            DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT()));

    private static final WatermarkStrategy<RowData> STRATEGY =
            WatermarkStrategy.<RowData>forMonotonousTimestamps()
                    .withWatermarkAlignment("aligned", Duration.ofMinutes(1));

    private static PubSubDynamicSource base() {
        return new PubSubDynamicSource(
                "catalog.db.source",
                PHYSICAL_DATA_TYPE,
                new DecodingFormatMock(",", true),
                Collections.singletonList(SubscriptionDestination.of("p", "events-sub")),
                Collections.emptyMap(),
                null,
                null,
                null,
                PubSubSubscriberOptions.defaults(),
                false,
                null,
                null,
                "localhost:8085",
                4);
    }

    private static PubSubWatermarkPushDownSource pushDown() {
        return pushDown(new Configuration(), new Configuration());
    }

    private static PubSubWatermarkPushDownSource pushDown(
            Configuration tableOptions, Configuration sessionConfig) {
        return PubSubWatermarkPushDownSource.of(base(), tableOptions, sessionConfig);
    }

    private static Configuration config(String key, String value) {
        Configuration configuration = new Configuration();
        configuration.setString(key, value);
        return configuration;
    }

    @Test
    void readsThroughTheSourceProviderUntilAStrategyIsPushed() {
        ScanTableSource.ScanRuntimeProvider provider =
                pushDown().getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider).isInstanceOf(SourceProvider.class);
        assertThat(((SourceProvider) provider).getParallelism()).contains(4);
    }

    @Test
    void readsAPushedStrategyThroughTheSourceItself() {
        PubSubWatermarkPushDownSource source = pushDown();
        source.applyWatermark(STRATEGY);

        ScanTableSource.ScanRuntimeProvider provider =
                source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

        assertThat(provider).isInstanceOf(DataStreamScanProvider.class);
        DataStreamScanProvider dataStreamProvider = (DataStreamScanProvider) provider;
        assertThat(dataStreamProvider.isBounded()).isFalse();
        assertThat(dataStreamProvider.getParallelism()).contains(4);

        DataStream<RowData> stream =
                dataStreamProvider.produceDataStream(
                        name -> Optional.of("7_" + name),
                        StreamExecutionEnvironment.getExecutionEnvironment());

        assertThat(stream.getTransformation()).isInstanceOf(SourceTransformation.class);
        SourceTransformation<?, ?, ?> transformation =
                (SourceTransformation<?, ?, ?>) stream.getTransformation();
        assertThat(transformation.getWatermarkStrategy()).isSameAs(STRATEGY);
        assertThat(transformation.getUid()).isEqualTo("7_source");
        assertThat(transformation.getOutputType())
                .isEqualTo(
                        ScanRuntimeProviderContext.INSTANCE.createTypeInformation(
                                PHYSICAL_DATA_TYPE));
    }

    @Test
    void leavesTheUidUnsetWhenThePlannerAssignsNone() {
        PubSubWatermarkPushDownSource source = pushDown();
        source.applyWatermark(STRATEGY);

        DataStream<RowData> stream =
                ((DataStreamScanProvider)
                                source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                        .produceDataStream(
                                name -> Optional.empty(),
                                StreamExecutionEnvironment.getExecutionEnvironment());

        assertThat(stream.getTransformation().getUid()).isNull();
    }

    @Test
    void aCopyCarriesThePushedStrategyAndStaysThePushDownSource() {
        PubSubWatermarkPushDownSource source = pushDown();
        source.applyWatermark(STRATEGY);

        assertThat(source.copy())
                .isInstanceOf(PubSubWatermarkPushDownSource.class)
                .isEqualTo(source)
                .hasSameHashCodeAs(source);
        assertThat(source).isNotEqualTo(pushDown());
        assertThat(pushDown()).isEqualTo(pushDown()).isNotEqualTo(base());
        assertThat(base()).isNotEqualTo(pushDown());
    }

    @Test
    void theTableIdleTimeoutIsPartOfTheIdentityAndTheSessionIsNot() {
        PubSubWatermarkPushDownSource none = pushDown();
        PubSubWatermarkPushDownSource table =
                pushDown(config("scan.watermark.idle-timeout", "30 s"), new Configuration());
        PubSubWatermarkPushDownSource session =
                pushDown(new Configuration(), config("table.exec.source.idle-timeout", "30 s"));

        assertThat(table).isNotEqualTo(none);
        assertThat(session).isEqualTo(none);
        assertThat(table.copy()).isEqualTo(table).hasSameHashCodeAs(table);
    }

    @Test
    void readsTheSessionIdleTimeoutWhenTheStrategyArrives() {
        Configuration session = new Configuration();
        PubSubWatermarkPushDownSource source = pushDown(new Configuration(), session);
        DynamicTableSource copy = source.copy();
        session.setString("table.exec.source.idle-timeout", "30 s");

        assertThatThrownBy(() -> ((PubSubWatermarkPushDownSource) copy).applyWatermark(STRATEGY))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "'table.exec.source.idle-timeout' = 'PT30S' would add source idleness");
    }

    @Test
    void acceptsAStrategyWhenNoIdleTimeoutIsSet() {
        PubSubWatermarkPushDownSource source = pushDown();

        source.applyWatermark(STRATEGY);

        assertThat(source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(DataStreamScanProvider.class);
    }

    @Test
    void rejectsTheSessionIdleTimeout() {
        PubSubWatermarkPushDownSource source =
                pushDown(new Configuration(), config("table.exec.source.idle-timeout", "30 s"));

        assertThatThrownBy(() -> source.applyWatermark(STRATEGY))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "'table.exec.source.idle-timeout' = 'PT30S' would add source idleness")
                .hasMessageContaining("FLINK-40234")
                .hasMessageContaining("Set 'scan.watermark.idle-timeout' = '0' on this table")
                .hasMessageContaining("set 'table.exec.source.idle-timeout' = '0' for this job");
    }

    @Test
    void rejectsTheTableIdleTimeoutAheadOfTheSessionOne() {
        PubSubWatermarkPushDownSource source =
                pushDown(
                        config("scan.watermark.idle-timeout", "1 min"),
                        config("table.exec.source.idle-timeout", "30 s"));

        assertThatThrownBy(() -> source.applyWatermark(STRATEGY))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "'scan.watermark.idle-timeout' = 'PT1M' would add source idleness");
    }

    @Test
    void aZeroTableIdleTimeoutOverridesTheSessionOne() {
        PubSubWatermarkPushDownSource source =
                pushDown(
                        config("scan.watermark.idle-timeout", "0 s"),
                        config("table.exec.source.idle-timeout", "30 s"));

        source.applyWatermark(STRATEGY);

        assertThat(source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(DataStreamScanProvider.class);
    }

    @Test
    void aZeroSessionIdleTimeoutAddsNoIdleness() {
        PubSubWatermarkPushDownSource source =
                pushDown(new Configuration(), config("table.exec.source.idle-timeout", "0 s"));

        source.applyWatermark(STRATEGY);

        assertThat(source.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE))
                .isInstanceOf(DataStreamScanProvider.class);
    }
}
