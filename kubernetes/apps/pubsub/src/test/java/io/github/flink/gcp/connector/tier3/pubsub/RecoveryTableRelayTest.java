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

package io.github.flink.gcp.connector.tier3.pubsub;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RecoveryTableRelayTest {
    @Test
    void everyTableOperatorHasTheSameExplicitUidAcrossTranslationsAndParallelism() {
        List<String> first = uids("table", 1);
        // A second translation in the same JVM advances the planner's static ExecNode counter.
        assertThat(uids("table", 2)).isEqualTo(first);
        assertThat(uids("table", 1)).isEqualTo(first);
        // Both translations have a table source scan and a sink, hence the per-translation prefix.
        assertThat(first)
                .containsExactly(
                        "pubsub-table-input-v1-stream-exec-calc-calc",
                        "pubsub-table-input-v1-stream-exec-sink-external-datastream",
                        "pubsub-table-input-v1-stream-exec-table-source-scan-source",
                        "pubsub-table-observer-v1",
                        "pubsub-table-output-v1-stream-exec-sink-sink",
                        "pubsub-table-output-v1-stream-exec-table-source-scan-source",
                        "pubsub-table-tagger-v1");
    }

    @Test
    void theDataStreamEntryPointKeepsItsTopology() {
        assertThat(uids("datastream", 2))
                .containsExactly("pubsub-input-v1", "pubsub-observer-v1", "pubsub-output-v1");
    }

    @Test
    void taggerChecksTheRowAgainstTheSubscriptionThatDeliveredIt() throws Exception {
        RecoveryOptions options = new RecoveryOptions("run-1", 2, 1, "initial", false, "table");
        RecoveryTableRelay.Tagger tagger = new RecoveryTableRelay.Tagger(options);
        Row row = Row.withNames();
        row.setField(
                "payload", RecoveryPayload.input(options, 1, 0).getBytes(StandardCharsets.UTF_8));
        row.setField("message_id", "m");
        row.setField("subscription", "projects/flink-gcp/subscriptions/t3-run-1-in-1");
        assertThat(tagger.map(row)).isEqualTo("v1|run-1|1|0|" + RecoveryPayload.encode("m"));
        row.setField("subscription", "projects/flink-gcp/subscriptions/t3-run-1-in-0");
        assertThatThrownBy(() -> tagger.map(row)).hasMessageContaining("subscription");
        row.setField("payload", null);
        assertThatThrownBy(() -> tagger.map(row)).hasMessageContaining("bounded UTF-8");
    }

    private static List<String> uids(String entryPoint, int parallelism) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        PubSubRecoveryJob.attach(
                env,
                new RecoveryOptions("uid-probe", 2, parallelism, "initial", false, entryPoint),
                "localhost:1");
        StreamGraph graph = env.getStreamGraph();
        // Job graph translation is where a repeated UID would be rejected.
        graph.getJobGraph();
        return graph.getStreamNodes().stream()
                .map(node -> String.valueOf(node.getTransformationUID()))
                .sorted()
                .collect(Collectors.toList());
    }
}
