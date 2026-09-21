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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** Explicit inputs for one finite measurement cell; admission and cleanup are external. */
@Internal
final class MeasurementOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Where a cluster session's evidence goes. It is the default because the reviewed session file
     * carries no such field: only a rig that writes elsewhere passes {@code --evidence-root}.
     */
    private static final String CLUSTER_EVIDENCE_ROOT = "gs://flink-gcp-cloudtasks-benchmark/runs/";

    enum Arm {
        UNNAMED,
        NAMED_HASH,
        NAMED_RANDOM_CONTROL,
        STAGED_HASH,
        STAGED_RANDOM
    }

    final String runId;
    final String cellId;
    final String queue;
    final String target;
    final Arm arm;
    final int bodyBytes;
    final int parallelism;
    final int concurrency;
    final int checkpointSeconds;
    final int channelPoolSize;
    final long records;
    final long warmup;
    final boolean skew;
    final double offeredRate;
    final long attemptLimit;
    final int warmupSeconds;
    final int observationSeconds;
    final int controlDelayMillis;
    final boolean emitAttempts;

    /** Where the receipts and rows go. The cluster never sets it; a local rig does. */
    final String evidenceRoot;

    private MeasurementOptions(Map<String, String> args) {
        Set<String> accepted =
                Set.of(
                        "run-id",
                        "cell-id",
                        "queue",
                        "target",
                        "arm",
                        "body-bytes",
                        "parallelism",
                        "concurrency",
                        "checkpoint-seconds",
                        "channel-pool-size",
                        "records",
                        "warmup-records",
                        "distribution",
                        "offered-rate",
                        "warmup-seconds",
                        "observation-seconds",
                        "record-limit",
                        "attempt-limit",
                        "control-delay-millis",
                        "emit-attempts",
                        "evidence-root");
        for (String key : args.keySet()) {
            if (!accepted.contains(key)) {
                throw new IllegalArgumentException("Unknown argument: --" + key);
            }
        }
        runId = label(required(args, "run-id"), "run-id");
        cellId = label(required(args, "cell-id"), "cell-id");
        queue = required(args, "queue");
        if (!queue.matches(
                "projects/flink-gcp/locations/us-central1/queues/ct1246-[a-z0-9-]{1,70}")) {
            throw new IllegalArgumentException(
                    "--queue must name the approved ct1246 queue in flink-gcp/us-central1");
        }
        target = required(args, "target");
        URI uri = URI.create(target);
        if (!"https".equals(uri.getScheme())
                || uri.getHost() == null
                || uri.getUserInfo() != null
                || uri.getFragment() != null
                || uri.getQuery() != null) {
            throw new IllegalArgumentException(
                    "--target must be HTTPS without credentials, query or fragment");
        }
        arm = Arm.valueOf(required(args, "arm"));
        bodyBytes =
                member(Integer.parseInt(required(args, "body-bytes")), "body-bytes", 1024, 65536);
        parallelism =
                member(Integer.parseInt(required(args, "parallelism")), "parallelism", 1, 4, 16);
        concurrency =
                member(Integer.parseInt(required(args, "concurrency")), "concurrency", 1, 4, 16);
        checkpointSeconds =
                member(
                        Integer.parseInt(required(args, "checkpoint-seconds")),
                        "checkpoint-seconds",
                        1,
                        10,
                        60);
        channelPoolSize =
                member(
                        Integer.parseInt(args.getOrDefault("channel-pool-size", "1")),
                        "channel-pool-size",
                        1,
                        4,
                        8);
        String distribution = required(args, "distribution");
        if (!distribution.equals("even") && !distribution.equals("skew")) {
            throw new IllegalArgumentException("--distribution must be even or skew");
        }
        skew = distribution.equals("skew");
        offeredRate = Double.parseDouble(required(args, "offered-rate"));
        if (!Double.isFinite(offeredRate) || offeredRate <= 0 || offeredRate > 10000) {
            throw new IllegalArgumentException(
                    "--offered-rate must be finite, positive and at most 10000");
        }
        if (args.containsKey("observation-seconds")) {
            if (args.containsKey("records") || args.containsKey("warmup-records")) {
                throw new IllegalArgumentException(
                        "Window inputs cannot include record-count mode");
            }
            warmupSeconds = Integer.parseInt(required(args, "warmup-seconds"));
            observationSeconds = Integer.parseInt(required(args, "observation-seconds"));
            if (warmupSeconds < 1
                    || warmupSeconds > 120
                    || observationSeconds < 1
                    || observationSeconds > 600) {
                throw new IllegalArgumentException(
                        "Require warmup-seconds 1..120 and observation-seconds 1..600");
            }
            // Two more nominal checkpoint intervals keep end-of-input flushing outside the
            // intended window. The external observer must still verify actual source/CP timing.
            records =
                    (long)
                            Math.ceil(
                                    offeredRate
                                            * (warmupSeconds
                                                    + observationSeconds
                                                    + 2L * checkpointSeconds
                                                    + 1));
            long limit = Long.parseLong(required(args, "record-limit"));
            if (limit < 1 || limit > 10000000 || records > limit) {
                throw new IllegalArgumentException(
                        "Window requires more than the approved record-limit (maximum 10000000)");
            }
            warmup = 0;
            attemptLimit = Long.parseLong(required(args, "attempt-limit"));
            if (attemptLimit < 1 || attemptLimit > 3 * records) {
                throw new IllegalArgumentException(
                        "Require 1 <= attempt-limit <= 3 * generated records");
            }
        } else {
            if (args.containsKey("warmup-seconds")
                    || args.containsKey("record-limit")
                    || args.containsKey("attempt-limit")) {
                throw new IllegalArgumentException("Window limits require observation-seconds");
            }
            warmupSeconds = 0;
            observationSeconds = 0;
            records = Long.parseLong(required(args, "records"));
            warmup = Long.parseLong(required(args, "warmup-records"));
            if (warmup < 1 || records <= warmup || records > 100000) {
                throw new IllegalArgumentException(
                        "Require 1 <= warmup-records < records <= 100000");
            }
            attemptLimit = 3 * records;
        }
        // The limit remains per creator incarnation. Admission must account for all subtasks
        // and authorized incarnations, including recovery; this is not a global RPC counter.
        controlDelayMillis =
                member(
                        Integer.parseInt(args.getOrDefault("control-delay-millis", "0")),
                        "control-delay-millis",
                        0,
                        100);
        String emit = args.getOrDefault("emit-attempts", "true");
        if (!emit.equals("true") && !emit.equals("false")) {
            throw new IllegalArgumentException("--emit-attempts must be true or false");
        }
        emitAttempts = emit.equals("true");
        evidenceRoot = args.getOrDefault("evidence-root", CLUSTER_EVIDENCE_ROOT);
        if (!evidenceRoot.endsWith("/")) {
            throw new IllegalArgumentException("--evidence-root must end with /");
        }
        URI root = URI.create(evidenceRoot);
        // An opaque URI hides a query in its scheme-specific part, and a two-slash file: root
        // silently turns its first path element into an authority and writes to the filesystem
        // root instead.
        if (root.getScheme() == null
                || root.isOpaque()
                || root.getPath() == null
                || root.getPath().isEmpty()
                || root.getQuery() != null
                || root.getFragment() != null) {
            throw new IllegalArgumentException("--evidence-root must be a plain hierarchical URI");
        }
        if ("file".equals(root.getScheme()) && root.getAuthority() != null) {
            throw new IllegalArgumentException("--evidence-root file URI must name no host");
        }
        if ((controlDelayMillis != 0 || !emitAttempts) && !windowed()) {
            throw new IllegalArgumentException("Calibration controls require window mode");
        }
        if (controlDelayMillis != 0
                && (arm != Arm.UNNAMED || parallelism != 1 || concurrency != 1 || !emitAttempts)) {
            throw new IllegalArgumentException(
                    "Delay control requires UNNAMED, parallelism/concurrency 1 and CSV output");
        }
    }

    boolean windowed() {
        return observationSeconds > 0;
    }

    String receiptPrefix() {
        return cellPrefix() + "receipts/";
    }

    String rowsPrefix() {
        return cellPrefix() + "rows/";
    }

    private String cellPrefix() {
        return evidenceRoot + runId + "/cells/" + cellId + "/";
    }

    static MeasurementOptions parse(String... args) {
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Expected --name value pairs");
        }
        Map<String, String> values = new HashMap<>();
        for (int index = 0; index < args.length; index += 2) {
            String option = args[index];
            if (!option.startsWith("--") || option.length() == 2) {
                throw new IllegalArgumentException("Expected an option starting with --");
            }
            if (values.putIfAbsent(option.substring(2), args[index + 1]) != null) {
                throw new IllegalArgumentException("Duplicate argument: " + option);
            }
        }
        return new MeasurementOptions(values);
    }

    int partition(long sequence, int partitions) {
        if (partitions == 1) {
            return 0;
        }
        if (skew) {
            return sequence % 10 < 9 ? 0 : 1 + (int) ((sequence / 10) % (partitions - 1));
        }
        return (int) (sequence % partitions);
    }

    private static String required(Map<String, String> args, String name) {
        String value = args.get(name);
        if (value == null) {
            throw new IllegalArgumentException("Missing --" + name);
        }
        return value;
    }

    private static String label(String value, String name) {
        if (!value.matches("[a-z0-9][a-z0-9-]{0,39}")) {
            throw new IllegalArgumentException(
                    "--" + name + " must be a 1-40 character lowercase label");
        }
        return value;
    }

    private static int member(int value, String name, int... supported) {
        for (int candidate : supported) {
            if (value == candidate) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unsupported --" + name);
    }
}
