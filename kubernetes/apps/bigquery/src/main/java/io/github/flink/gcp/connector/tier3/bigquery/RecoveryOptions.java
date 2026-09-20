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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ParameterTool;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/** Fixed-target inputs for one finite BigQuery recovery trial. */
@Internal
final class RecoveryOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String PROJECT = "flink-gcp";
    static final String DATASET = "flink_gcp_tier3_bigquery";
    static final long MAX_BYTES_PER_SECOND = 1024 * 1024;
    static final long MAX_BYTES = 2L * 1024 * 1024 * 1024;

    enum Mode {
        ALO(64 * 1024),
        EO(1024);

        final int rowBytes;

        Mode(int rowBytes) {
            this.rowBytes = rowBytes;
        }
    }

    final String runId;
    final Mode mode;
    final int destinations;
    final long records;
    final long bytesPerSecond;
    final String phase;
    final boolean requireRestored;
    private final TableDestination[] tables;

    private RecoveryOptions(ParameterTool args) {
        runId = args.getRequired("run-id");
        if (!runId.matches("[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?")) {
            throw new IllegalArgumentException("--run-id must match the Tier-3 run label grammar");
        }
        mode = Mode.valueOf(args.getRequired("mode"));
        destinations = args.getInt("destinations");
        if (destinations != 10 && destinations != 50) {
            throw new IllegalArgumentException("--destinations must be 10 or 50");
        }
        records = args.getLong("records", 1800 * MAX_BYTES_PER_SECOND / mode.rowBytes);
        if (records < 2L * destinations || records > MAX_BYTES / mode.rowBytes) {
            throw new IllegalArgumentException(
                    "--records must cover all destinations on both writers and fit 2 GiB");
        }
        bytesPerSecond = args.getLong("bytes-per-second", MAX_BYTES_PER_SECOND);
        if (bytesPerSecond < 1 || bytesPerSecond > MAX_BYTES_PER_SECOND) {
            throw new IllegalArgumentException("--bytes-per-second must be between 1 and 1048576");
        }
        phase = args.get("phase", "initial");
        if (!Set.of("initial", "upgrade").contains(phase)) {
            throw new IllegalArgumentException("--phase must be initial or upgrade");
        }
        String restored = args.get("require-restored", "false");
        if (!Set.of("true", "false").contains(restored)) {
            throw new IllegalArgumentException("--require-restored must be true or false");
        }
        requireRestored = Boolean.parseBoolean(restored);
        if (phase.equals("upgrade") && !requireRestored) {
            throw new IllegalArgumentException("--phase upgrade requires --require-restored true");
        }
        tables = new TableDestination[destinations];
        String prefix = "bq_" + runId.replace('-', '_') + "_d";
        for (int destination = 0; destination < destinations; destination++) {
            tables[destination] = TableDestination.of(PROJECT, DATASET, prefix + destination);
        }
    }

    static RecoveryOptions parse(String... args) {
        Set<String> allowed =
                Set.of(
                        "run-id",
                        "mode",
                        "destinations",
                        "records",
                        "bytes-per-second",
                        "phase",
                        "require-restored");
        Set<String> seen = new HashSet<>();
        if (args.length % 2 != 0) {
            throw new IllegalArgumentException("Arguments must be --name value pairs");
        }
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--")
                    || !allowed.contains(args[i].substring(2))
                    || !seen.add(args[i])) {
                throw new IllegalArgumentException(
                        "Unknown or duplicate BigQuery argument: " + args[i]);
            }
        }
        return new RecoveryOptions(ParameterTool.fromArgs(args));
    }

    int destination(long sequence) {
        if (sequence < 0 || sequence >= records) {
            throw new IllegalArgumentException("Sequence is outside this run's input");
        }
        return (int) (sequence % destinations);
    }

    TableDestination table(long sequence) {
        return tables[destination(sequence)];
    }

    String identity() {
        // Phase changes during upgrade; the input and sink contract must not change.
        return "v1/"
                + runId
                + "/"
                + mode
                + "/"
                + destinations
                + "/"
                + records
                + "/"
                + bytesPerSecond;
    }
}
