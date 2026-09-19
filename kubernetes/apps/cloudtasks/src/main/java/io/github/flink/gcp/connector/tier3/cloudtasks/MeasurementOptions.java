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
                        "offered-rate");
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
        records = Long.parseLong(required(args, "records"));
        warmup = Long.parseLong(required(args, "warmup-records"));
        if (warmup < 1 || records <= warmup || records > 100000) {
            throw new IllegalArgumentException("Require 1 <= warmup-records < records <= 100000");
        }
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
        // Per creator incarnation, including every retry. External admission must multiply by
        // parallelism and the explicitly allowed number of process incarnations for its budget.
        attemptLimit = 3 * records;
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
