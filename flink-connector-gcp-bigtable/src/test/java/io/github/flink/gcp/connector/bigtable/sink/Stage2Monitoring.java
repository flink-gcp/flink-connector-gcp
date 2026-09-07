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

import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

/** Read-only, exact-instance Cloud Monitoring capture; it can run after resource deletion. */
final class Stage2Monitoring {
    private Stage2Monitoring() {}

    static void capture(Stage2Lease lease) throws Exception {
        long maxBytes = 8L * 1024 * 1024;
        String start = Instant.ofEpochMilli(lease.startedAt()).toString();
        String end = Instant.now().toString();
        Path output = lease.manifest.resolveSibling("monitoring.jsonl");
        long bytes = 0;
        try (var writer = openCapture(lease, output, maxBytes)) {
            GoogleCredentials credentials =
                    GoogleCredentials.getApplicationDefault()
                            .createScoped("https://www.googleapis.com/auth/monitoring.read");
            credentials.refreshIfExpired();
            HttpClient http = HttpClient.newHttpClient();
            var mapper =
                    new org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind
                            .ObjectMapper();
            for (String metric :
                    new String[] {
                        "cluster/cpu_load",
                        "cluster/cpu_load_hottest_node",
                        "server/latencies",
                        "server/request_count",
                        "table/bytes_used",
                        "disk/bytes_used"
                    }) {
                String filter =
                        "metric.type=\"bigtable.googleapis.com/"
                                + metric
                                + "\" AND resource.labels.instance=\""
                                + lease.instance
                                + "\"";
                String page = "";
                do {
                    String query =
                            "filter="
                                    + encode(filter)
                                    + "&interval.startTime="
                                    + encode(start)
                                    + "&interval.endTime="
                                    + encode(end)
                                    + "&view=FULL&pageSize=1000"
                                    + (page.isEmpty() ? "" : "&pageToken=" + encode(page));
                    HttpResponse<String> response =
                            http.send(
                                    HttpRequest.newBuilder(
                                                    URI.create(
                                                            "https://monitoring.googleapis.com/v3/projects/flink-gcp/timeSeries?"
                                                                    + query))
                                            .header(
                                                    "Authorization",
                                                    "Bearer "
                                                            + credentials
                                                                    .getAccessToken()
                                                                    .getTokenValue())
                                            .timeout(Duration.ofSeconds(30))
                                            .build(),
                                    HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() != 200) {
                        throw new IOException(
                                "Stage 2 Monitoring read failed: HTTP " + response.statusCode());
                    }
                    bytes += response.body().getBytes(StandardCharsets.UTF_8).length;
                    if (bytes > maxBytes) {
                        throw new IOException("Monitoring capture byte cap exhausted");
                    }
                    var body = mapper.readTree(response.body());
                    var record = mapper.createObjectNode();
                    record.put("metric", metric);
                    record.put("start", start);
                    record.put("end", end);
                    record.set("response", body);
                    writer.write(mapper.writeValueAsString(record));
                    writer.newLine();
                    page = body.path("nextPageToken").asText("");
                } while (!page.isEmpty());
            }
        }
        System.out.println(
                "STAGE2_MONITORING capturedBytes="
                        + bytes
                        + " missing series remain unmeasured; physical storage is not inferred from logical bytes");
    }

    static java.io.BufferedWriter openCapture(Stage2Lease lease, Path output, long maxBytes)
            throws IOException {
        var writer = Files.newBufferedWriter(output, java.nio.file.StandardOpenOption.CREATE_NEW);
        try {
            lease.reserveRead(maxBytes);
            return writer;
        } catch (IOException | RuntimeException | Error failure) {
            try {
                writer.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
