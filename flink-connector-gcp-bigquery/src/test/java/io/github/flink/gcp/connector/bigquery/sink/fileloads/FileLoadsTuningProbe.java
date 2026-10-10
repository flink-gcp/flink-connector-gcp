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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.bigquery.RealTables;
import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.testutils.TestNames;
import io.github.flink.gcp.connector.testutils.bigquery.RealBigQuery;
import io.github.flink.gcp.connector.testutils.bigquery.RealGcs;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * The in-region tuning probe of issue #1313: one streaming FILE_LOADS job per invocation, through
 * real Flink checkpoints, real Cloud Storage staging and real BigQuery load jobs. It records each
 * completed checkpoint's writer timings and writes them, with the cell and its exact-count result,
 * to one JSON file.
 *
 * <p>Not a test, and named so that neither surefire execution selects it. It is run by hand on a VM
 * in the gated dataset's region, with the three variables {@link BigQueryFileLoadsITCase} reads,
 * and its arguments are {@code key=value} pairs read by {@link #main}. The load jobs are not read
 * here: the gated service account holds {@code bigquery.jobs.create} but not {@code
 * bigquery.jobs.list}, so they are listed afterwards by a principal that may, within the recorded
 * window and table prefix.
 *
 * <p>The source is gated per checkpoint, emitting {@code destinations * bytesPerDestination /
 * rowBytes} rows and then waiting for a checkpoint to complete. The gate reopens on completion
 * rather than at the barrier, so a checkpoint that triggers before the quota is out carries only
 * part of it, and the next carries the rest together with the following quota. The analysis keeps
 * only checkpoints whose load jobs carried exactly one quota. Rows are partitioned by destination,
 * so that each writer subtask holds the destinations congruent to its index and sink parallelism
 * spreads destinations rather than multiplying files.
 *
 * <p>Flink runs the writer's finalization in its pre-barrier step, which neither the synchronous
 * nor the asynchronous duration includes. A writer subtask's finalization is therefore read as its
 * end-to-end duration less the barrier's start delay, alignment, and both snapshot parts.
 */
final class FileLoadsTuningProbe {

    private static final int REST_PORT = 18081;

    private static final TableSchema SCHEMA =
            TableSchema.newBuilder()
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("id")
                                    .setType(TableFieldSchema.Type.INT64)
                                    .setMode(TableFieldSchema.Mode.REQUIRED))
                    .addFields(
                            TableFieldSchema.newBuilder()
                                    .setName("payload")
                                    .setType(TableFieldSchema.Type.BYTES)
                                    .setMode(TableFieldSchema.Mode.REQUIRED))
                    .build();

    private FileLoadsTuningProbe() {}

    /** How compressible a row's payload is. */
    private enum Shape {
        /** Uniformly random bytes, which neither staging format can compress. */
        RANDOM,
        /** Random hexadecimal digits: four bits of entropy per byte, about half compressible. */
        HEX
    }

    /** Writes {@code id} and a payload derived from it, so a row is reproducible from its id. */
    private static final class PayloadSerializer extends FixedSchemaProtoSerializer<Long> {
        private static final long serialVersionUID = 1L;
        private static final byte[] HEX_DIGITS =
                "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

        private final Shape shape;
        private final int rowBytes;

        PayloadSerializer(Shape shape, int rowBytes) {
            this.shape = shape;
            this.rowBytes = rowBytes;
        }

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return SCHEMA;
        }

        @Override
        public ByteString serialize(Long id) {
            return DynamicMessage.newBuilder(descriptor())
                    .setField(field("id"), id)
                    .setField(field("payload"), ByteString.copyFrom(payload(id)))
                    .build()
                    .toByteString();
        }

        private byte[] payload(long id) {
            SplittableRandom random = new SplittableRandom(id);
            byte[] bytes = new byte[rowBytes];
            if (shape == Shape.RANDOM) {
                random.nextBytes(bytes);
                return bytes;
            }
            for (int i = 0; i < bytes.length; i += 16) {
                long digits = random.nextLong();
                for (int j = i; j < Math.min(i + 16, bytes.length); j++) {
                    bytes[j] = HEX_DIGITS[(int) (digits & 0xF)];
                    digits >>>= 4;
                }
            }
            return bytes;
        }
    }

    /**
     * Runs one cell. Required: {@code cell}, {@code destinations}, {@code concurrency}, {@code
     * bytesPerDestination}, {@code checkpoints}, {@code out}. Optional: {@code parallelism} (1),
     * {@code format} ({@code AVRO}), {@code shape} ({@code RANDOM}), {@code rowBytes} (1024),
     * {@code intervalSeconds} (30).
     */
    public static void main(String[] args) throws Exception {
        Map<String, String> arguments = new HashMap<>();
        for (String argument : args) {
            int equals = argument.indexOf('=');
            arguments.put(argument.substring(0, equals), argument.substring(equals + 1));
        }
        String cell = arguments.get("cell");
        int destinations = Integer.parseInt(arguments.get("destinations"));
        int concurrency = Integer.parseInt(arguments.get("concurrency"));
        long bytesPerDestination = Long.parseLong(arguments.get("bytesPerDestination"));
        int checkpoints = Integer.parseInt(arguments.get("checkpoints"));
        Path out = Path.of(arguments.get("out"));
        int parallelism = Integer.parseInt(arguments.getOrDefault("parallelism", "1"));
        StagingFormat format = StagingFormat.valueOf(arguments.getOrDefault("format", "AVRO"));
        Shape shape = Shape.valueOf(arguments.getOrDefault("shape", "RANDOM"));
        int rowBytes = Integer.parseInt(arguments.getOrDefault("rowBytes", "1024"));
        Duration interval =
                Duration.ofSeconds(Long.parseLong(arguments.getOrDefault("intervalSeconds", "30")));

        int rowsPerCheckpoint = Math.toIntExact(destinations * (bytesPerDestination / rowBytes));
        long rows = (long) rowsPerCheckpoint * checkpoints;
        if (rowsPerCheckpoint % parallelism != 0) {
            throw new IllegalArgumentException("rows per checkpoint must divide by parallelism");
        }
        String prefix = "tuning_" + TestNames.runId() + "_" + cell;
        String staging = "flink-file-loads-tuning/" + prefix;

        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT, String.valueOf(REST_PORT));
        configuration.set(WebOptions.CHECKPOINTS_HISTORY_SIZE, 1000);
        // A restart would replay a batch and inflate the timings unseen; fail the cell instead.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(parallelism, configuration);
        env.enableCheckpointing(interval.toMillis());

        DataGeneratorSource<Long> source =
                new DataGeneratorSource<>(
                        id -> id,
                        rows,
                        RateLimiterStrategy.perCheckpoint(rowsPerCheckpoint),
                        Types.LONG);
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "rows")
                .partitionCustom(
                        (destination, subtasks) -> destination % subtasks,
                        id -> (int) (id % destinations))
                .sinkTo(
                        BigQuerySink.<Long>builder()
                                .writeMethod(WriteMethod.FILE_LOADS)
                                .destinationResolver(
                                        (id, context) ->
                                                RealTables.destination(
                                                        table(prefix, id % destinations)))
                                .serializer(new PayloadSerializer(shape, rowBytes))
                                .fileLoadsOptions(
                                        FileLoadsOptions.builder()
                                                .stagingPath(RealGcs.uri(staging))
                                                .minCheckpointInterval(interval)
                                                .stagingFormat(format)
                                                .maxConcurrentCheckpointFinalizations(concurrency)
                                                // The default 16 would evict and re-open files
                                                // on every row once a writer holds 50.
                                                .maxOpenDestinations(64)
                                                .build())
                                .build());

        // Tables and staging are deleted whether or not the job and its checks succeed.
        Instant started = Instant.now();
        JobClient job = env.executeAsync(cell);
        // The result future rather than the job status: the cluster shuts down as the job ends,
        // and a status request after that throws.
        var result = job.getJobExecutionResult();
        try {
            JsonObject checkpointRecords = new JsonObject();
            String base = "http://localhost:" + REST_PORT + "/jobs/" + job.getJobID();
            HttpClient http = HttpClient.newHttpClient();
            String writerVertex = null;
            while (!result.isDone()) {
                Thread.sleep(1000);
                try {
                    if (writerVertex == null) {
                        writerVertex = writerVertex(get(http, base));
                    }
                    recordCompleted(http, base, writerVertex, checkpointRecords);
                } catch (IOException e) {
                    // The cluster shuts down as the job finishes; the final checkpoint may be lost.
                    System.out.println("rest unavailable: " + e);
                }
            }
            result.get();
            Instant finished = Instant.now();

            StringBuilder counts = new StringBuilder("SELECT SUM(n) FROM (");
            for (int i = 0; i < destinations; i++) {
                counts.append(i == 0 ? "" : " UNION ALL ")
                        .append("SELECT COUNT(*) AS n FROM ")
                        .append(RealBigQuery.tablePath(table(prefix, i)));
            }
            long loaded = RealBigQuery.queryLongs(counts.append(")").toString()).get(0);
            int leftovers = 0;
            for (var ignored : RealGcs.list(staging)) {
                leftovers++;
            }

            JsonObject record = new JsonObject();
            arguments.forEach(record::addProperty);
            record.addProperty("tablePrefix", prefix);
            record.addProperty("started", started.toString());
            record.addProperty("finished", finished.toString());
            record.addProperty("rowsExpected", rows);
            record.addProperty("rowsLoaded", loaded);
            record.addProperty("stagingLeftovers", leftovers);
            // Not "checkpoints", the requested count copied from the arguments above.
            record.add("checkpointStats", checkpointRecords);
            Files.createDirectories(out);
            Files.writeString(out.resolve(cell + ".json"), new Gson().toJson(record));
            System.out.println(
                    cell
                            + " rows "
                            + loaded
                            + "/"
                            + rows
                            + " leftovers "
                            + leftovers
                            + " checkpoints "
                            + checkpointRecords.size());

        } finally {
            if (!result.isDone()) {
                // Settle the job first, or its later checkpoints would recreate what is deleted.
                job.cancel();
                try {
                    result.get();
                } catch (ExecutionException | CancellationException e) {
                    // Expected after the cancel; the failure that got here propagates instead.
                }
            }
            String[] tables = new String[destinations];
            for (int i = 0; i < destinations; i++) {
                tables[i] = table(prefix, i);
            }
            Closers.closeAll(
                    () -> RealBigQuery.deleteTables(tables), () -> RealGcs.deletePrefix(staging));
        }
    }

    private static String table(String prefix, long index) {
        return prefix + "_" + index;
    }

    private static String writerVertex(JsonObject jobDetails) {
        for (JsonElement vertex : jobDetails.getAsJsonArray("vertices")) {
            String name = vertex.getAsJsonObject().get("name").getAsString();
            if (name.contains("Writer")) {
                return vertex.getAsJsonObject().get("id").getAsString();
            }
        }
        throw new IllegalStateException("no writer vertex in " + jobDetails);
    }

    private static void recordCompleted(
            HttpClient http, String base, String writerVertex, JsonObject records)
            throws IOException, InterruptedException {
        JsonArray history = get(http, base + "/checkpoints").getAsJsonArray("history");
        for (JsonElement element : history) {
            JsonObject checkpoint = element.getAsJsonObject();
            String id = checkpoint.get("id").getAsString();
            if (!"COMPLETED".equals(checkpoint.get("status").getAsString()) || records.has(id)) {
                continue;
            }
            JsonObject vertex =
                    get(http, base + "/checkpoints/details/" + id + "/subtasks/" + writerVertex);
            TreeMap<Integer, Long> finalization = new TreeMap<>();
            for (JsonElement subtaskElement : vertex.getAsJsonArray("subtasks")) {
                JsonObject subtask = subtaskElement.getAsJsonObject();
                long remainder =
                        subtask.get("end_to_end_duration").getAsLong()
                                - subtask.get("start_delay").getAsLong()
                                - subtask.getAsJsonObject("alignment").get("duration").getAsLong()
                                - subtask.getAsJsonObject("checkpoint").get("sync").getAsLong()
                                - subtask.getAsJsonObject("checkpoint").get("async").getAsLong();
                finalization.put(subtask.get("index").getAsInt(), remainder);
            }
            JsonObject record = new JsonObject();
            record.addProperty("endToEndMillis", checkpoint.get("end_to_end_duration").getAsLong());
            record.addProperty("triggered", checkpoint.get("trigger_timestamp").getAsLong());
            record.add("writerFinalizationMillis", new Gson().toJsonTree(finalization.values()));
            record.add("writerSubtasks", vertex.getAsJsonArray("subtasks"));
            records.add(id, record);
        }
    }

    private static JsonObject get(HttpClient http, String uri)
            throws IOException, InterruptedException {
        HttpResponse<String> response =
                http.send(
                        HttpRequest.newBuilder(URI.create(uri)).build(),
                        HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException(uri + " returned " + response.statusCode());
        }
        return JsonParser.parseString(response.body()).getAsJsonObject();
    }
}
