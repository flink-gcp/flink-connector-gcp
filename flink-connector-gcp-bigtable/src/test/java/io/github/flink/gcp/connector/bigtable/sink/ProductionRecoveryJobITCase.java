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

package io.github.flink.gcp.connector.bigtable.sink;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Timeout(180)
class ProductionRecoveryJobITCase {
    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void serviceScenarioRecoversBothFactoriesAndRejectsDoubledSum(boolean tableApi)
            throws Exception {
        var backend = new ProductionRecoveryProxyTest.FakeBackend();
        try (var run =
                        ProductionRecoveryJob.newRun(
                                io.github.flink.gcp.connector.bigtable.TableDestination.of(
                                        "local-project", "local-instance", "local-table"));
                var proxy =
                        new ProductionRecoveryProxy(
                                backend,
                                LocalStagedHarness.tableName(run.table),
                                LocalStagedHarness.PROFILE)) {
            ProductionRecoveryJob.run(
                    run,
                    proxy,
                    directory,
                    tableApi,
                    () ->
                            BigtableProductionRecoveryProbe.verifyReadback(
                                    run, proxy, backend.rows()));
            assertThat(proxy.discarded).isEqualTo(1);
            assertThat(backend.probe.applied).isEqualTo(128);
            assertThat(proxy.duplicates).isGreaterThanOrEqualTo(257);
            var first = backend.probe.sent.get(0);
            var withoutMarker =
                    first.toBuilder()
                            .setPredicateFilter(
                                    com.google.bigtable.v2.RowFilter.newBuilder()
                                            .setChain(
                                                    com.google.bigtable.v2.RowFilter.Chain
                                                            .newBuilder()
                                                            .addFilters(
                                                                    com.google.bigtable.v2.RowFilter
                                                                            .newBuilder()
                                                                            .setFamilyNameRegexFilter(
                                                                                    "never-present"))
                                                            .addFilters(
                                                                    com.google.bigtable.v2.RowFilter
                                                                            .newBuilder()
                                                                            .setColumnQualifierRegexFilter(
                                                                                    com.google
                                                                                            .protobuf
                                                                                            .ByteString
                                                                                            .copyFromUtf8(
                                                                                                    ".*")))))
                            .build();
            backend.probe.apply(withoutMarker);
            assertThatThrownBy(
                            () ->
                                    BigtableProductionRecoveryProbe.verifyReadback(
                                            run, proxy, backend.rows()))
                    .hasMessageContaining("SUM differs");
            assertThatThrownBy(
                            () ->
                                    BigtableProductionRecoveryProbe.verifyReadback(
                                            run, proxy, java.util.List.of()))
                    .hasMessageContaining("empty or incomplete");
        }
    }
}
