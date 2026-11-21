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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class RecoveryTopologyTest {
    @Test
    void theRelayKeepsItsTopology() {
        assertThat(uids(2))
                .containsExactly("pubsub-input-v1", "pubsub-observer-v1", "pubsub-output-v1");
    }

    private static List<String> uids(int parallelism) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        PubSubRecoveryJob.attach(
                env,
                new RecoveryOptions("uid-probe", 2, parallelism, "initial", false),
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
