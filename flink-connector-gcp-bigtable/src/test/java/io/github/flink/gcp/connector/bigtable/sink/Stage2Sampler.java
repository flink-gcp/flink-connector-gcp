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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** REST sampling preserves missing metrics as missing, rather than inventing zero backlog. */
final class Stage2Sampler {
    private final URI address;
    private final String job;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper json = new ObjectMapper();
    private final Map<String, String> metricPaths = new LinkedHashMap<>();

    Stage2Sampler(LocalStagedJob job) throws Exception {
        address = job.cluster.getRestAddress().get(10, TimeUnit.SECONDS);
        this.job = "/jobs/" + job.client.getJobID();
    }

    String sample() throws Exception {
        // Discover again because committer metrics can register after task metrics.
        metricPaths.clear();
        {
            for (JsonNode vertex : json.readTree(get(job)).path("vertices")) {
                String path = job + "/vertices/" + vertex.path("id").asText() + "/subtasks/metrics";
                List<String> names = metricNames(json.readTree(get(path)));
                if (!names.isEmpty()) {
                    metricPaths.put(path, String.join(",", names));
                }
            }
        }
        Map<String, JsonNode> metrics = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : metricPaths.entrySet()) {
            metrics.put(
                    entry.getKey(),
                    json.readTree(
                            get(
                                    entry.getKey()
                                            + "?get="
                                            + URLEncoder.encode(
                                                    entry.getValue(), StandardCharsets.UTF_8))));
        }
        return json.writeValueAsString(metrics);
    }

    static List<String> metricNames(JsonNode metrics) {
        List<String> names = new ArrayList<>();
        for (JsonNode metric : metrics) {
            String name = metric.path("id").asText();
            if (name.endsWith("pendingCommittables")
                    || name.endsWith("backPressuredTimeMsPerSecond")
                    || name.endsWith("busyTimeMsPerSecond")
                    || name.endsWith("idleTimeMsPerSecond")) {
                names.add(name);
            }
        }
        return names;
    }

    private String get(String path) throws Exception {
        HttpResponse<String> response =
                http.send(
                        HttpRequest.newBuilder(address.resolve(path))
                                .timeout(Duration.ofSeconds(10))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200 || response.body().length() > 1_000_000) {
            throw new IOException("Stage 2 REST sampling failed: " + response.statusCode());
        }
        return response.body();
    }
}
