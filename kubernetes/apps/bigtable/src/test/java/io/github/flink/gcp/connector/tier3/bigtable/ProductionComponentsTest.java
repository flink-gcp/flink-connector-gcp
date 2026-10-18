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

package io.github.flink.gcp.connector.tier3.bigtable;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;

import io.github.flink.gcp.connector.bigtable.sink.BigtableMutateRowsSink;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkConfig;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableStagedSink;
import io.github.flink.gcp.connector.bigtable.source.BigtableChangeStreamSource;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins what the production connectors are built to do, since none of them runs locally: which table
 * each reaches, in which mode, under which profile, and what each sink writes.
 */
class ProductionComponentsTest {
    private static final RecoveryOptions OPTIONS =
            RecoveryOptions.parse(RecoveryOptionsTest.arguments());
    private static final BigtableRecoveryJob.Components PRODUCTION =
            BigtableRecoveryJob.Components.production(OPTIONS);

    @Test
    @SuppressWarnings("unchecked")
    void theExactlyOnceSinkStagesContributionsIntoEoUnderTheTransactionalProfile()
            throws Exception {
        assertThat(PRODUCTION.exactlyOnce()).isInstanceOf(BigtableStagedSink.class);
        var sink = (BigtableStagedSink<Long>) PRODUCTION.exactlyOnce();
        assertThat(sink.getStagedOptions().getMarkerFamily()).isEqualTo("flink_commit");
        assertThat(sink.getConfig().getAppProfileId()).isEqualTo("recovery");
        assertThat(sink.getConfig().getSerializer().serialize(42L, null).toProto())
                .isEqualTo(OPTIONS.contribution(42).toProto());
        assertThat(table(sink)).isEqualTo("eo");
    }

    @Test
    @SuppressWarnings("unchecked")
    void theAtLeastOnceSinkWritesTheSameContributionsIntoAlo() throws Exception {
        assertThat(PRODUCTION.atLeastOnce()).isInstanceOf(BigtableMutateRowsSink.class);
        BigtableSinkConfig<Long> config =
                ((BigtableMutateRowsSink<Long>) PRODUCTION.atLeastOnce()).getConfig();
        assertThat(config.getAppProfileId()).isEqualTo("recovery");
        assertThat(config.getSerializer().serialize(42L, null).toProto())
                .isEqualTo(OPTIONS.contribution(42).toProto());
        assertThat(table(PRODUCTION.atLeastOnce())).isEqualTo("alo");
    }

    @Test
    @SuppressWarnings("unchecked")
    void theRelaySinkWritesEachDeliveryIntoRelay() throws Exception {
        assertThat(PRODUCTION.relay()).isInstanceOf(BigtableMutateRowsSink.class);
        BigtableSinkConfig<Tuple3<String, String, Long>> config =
                ((BigtableMutateRowsSink<Tuple3<String, String, Long>>) PRODUCTION.relay())
                        .getConfig();
        var delivery = Tuple3.of("a/0000042", "initial/instance", 7L);
        assertThat(config.getAppProfileId()).isEqualTo("recovery");
        assertThat(config.getSerializer().serialize(delivery, null).toProto())
                .isEqualTo(ChangeRelay.mutation(delivery).toProto());
        assertThat(table(PRODUCTION.relay())).isEqualTo("relay");
    }

    /** The start position and app profile are not visible outside the connector's package. */
    @Test
    void theChangeStreamReadsEoAndIsBoundedSoTheJobFinishes() {
        assertThat(PRODUCTION.changeStream()).isInstanceOf(BigtableChangeStreamSource.class);
        assertThat(PRODUCTION.changeStream().getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        assertThat(table(PRODUCTION.changeStream())).isEqualTo("eo");
    }

    /** The one table a connector's lineage names, after checking it is in the run's instance. */
    private static String table(Object connector) {
        LineageVertex vertex = ((LineageVertexProvider) connector).getLineageVertex();
        assertThat(vertex.datasets()).hasSize(1);
        var dataset = vertex.datasets().get(0);
        assertThat(List.of(dataset.namespace()))
                .containsExactly("bigtable://flink-gcp/bt1316-bigtable-it");
        return dataset.name();
    }
}
