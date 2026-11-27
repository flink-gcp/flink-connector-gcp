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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.RunQueryRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.AbstractDatastoreEmulatorITCase;
import io.github.flink.gcp.connector.datastore.source.DatastoreSource;
import io.github.flink.gcp.connector.datastore.source.DatastoreSourceConfig;
import io.github.flink.gcp.connector.datastore.source.TestSources;
import io.github.flink.gcp.connector.datastore.source.batch.DatastoreBatchSource;
import io.github.flink.gcp.connector.datastore.source.batch.SplittableQueries;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The client library's splitter reaches the emulator over its HTTP client through every endpoint
 * form this class lists: {@code host:port}, gRPC's {@code dns} scheme ({@code dns:///}, {@code
 * dns:/}, {@code dns:} and {@code dns://<name server>/}), and an {@code http://} or {@code
 * https://} scheme (#2338).
 *
 * <p>Each form is checked twice: a query sent straight through the splitter's HTTP client, and a
 * plan asking for several ranges, whose planner's {@code split} is wrapped to witness that it was
 * reached and returned. Why each prefixed form needs the repair is recorded on {@code
 * SplitterClient.httpAuthority} and in ADR-0208; how many ranges the emulator's sampling answers is
 * not what this holds.
 */
class SplitterClientITCase extends AbstractDatastoreEmulatorITCase {

    private static final int SPLIT_COUNT = 4;

    /** Every endpoint form both tests run with; a new form the gRPC clients accept goes here. */
    static Stream<String> endpointPrefixes() {
        return Stream.of("", "dns:///", "dns:/", "dns:", "dns://127.0.0.1/", "http://", "https://");
    }

    @ParameterizedTest(name = "endpoint prefix \"{0}\"")
    @MethodSource("endpointPrefixes")
    void theSplittersHttpClientIsAnswered(String prefix) throws Exception {
        String kind = uniqueKind();
        RunQueryRequest request =
                RunQueryRequest.newBuilder()
                        .setProjectId(PROJECT)
                        .setPartitionId(PartitionId.newBuilder().setProjectId(PROJECT))
                        .setQuery(SplittableQueries.ofKind(kind))
                        .build();

        try (SplitterClient client =
                new SplitterClient(
                        EmulatorEndpoint.parse(prefix + emulatorEndpoint(), "emulatorEndpoint"))) {
            assertThat(client.get(PROJECT).runQuery(request).getBatch().getEntityResultsList())
                    .isEmpty();
        }
    }

    @ParameterizedTest(name = "endpoint prefix \"{0}\"")
    @MethodSource("endpointPrefixes")
    void aPlanThatCutsKeyRangesReachesTheSplitter(String prefix) throws Exception {
        String endpoint = prefix + emulatorEndpoint();
        DatastoreSourceConfig<String> config =
                ((DatastoreBatchSource<String>)
                                DatastoreSource.<String>builder()
                                        .database(database())
                                        .deserializer(new TestSources.KeyNameDeserializer())
                                        .emulatorEndpoint(endpoint)
                                        .kind(uniqueKind())
                                        .splitCount(SPLIT_COUNT)
                                        .build())
                        .getConfig();
        AtomicInteger splits = new AtomicInteger();

        try (ClientQueryPlanner planner =
                new ClientQueryPlanner(EmulatorEndpoint.parse(endpoint, "emulatorEndpoint")) {
                    @Override
                    protected List<Query> split(Query query, PartitionId partition, int splitCount)
                            throws IOException {
                        List<Query> ranges = super.split(query, partition, splitCount);
                        splits.incrementAndGet();
                        return ranges;
                    }
                }) {
            assertThat(planner.plan(config, 1).getRequests()).hasSizeBetween(1, SPLIT_COUNT);
        }
        assertThat(splits).as("splitter calls answered").hasValue(1);
    }
}
