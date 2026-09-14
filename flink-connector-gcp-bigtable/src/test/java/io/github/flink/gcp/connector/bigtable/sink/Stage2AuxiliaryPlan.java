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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/** Separate reservations for the serialized control and sustained hot-row observation. */
final class Stage2AuxiliaryPlan {
    static final long WARMUP_SECONDS = 10;
    static final List<String> NAMES = List.of("serialized", "sustained");
    private static final List<String> LIMITS =
            List.of(
                    "inventoryEntries",
                    "inventoryBytes",
                    "stagedEntries",
                    "stagedBytes",
                    "workBytes",
                    "checkpointTimeoutMillis",
                    "drainMillis");
    final String sha256;
    final List<Phase> phases;

    static Stage2AuxiliaryPlan read(Path directory, Properties campaign) throws IOException {
        try {
            byte[] snapshot = snapshot(directory);
            return snapshot == null ? null : new Stage2AuxiliaryPlan(snapshot, campaign);
        } catch (IllegalArgumentException | ArithmeticException failure) {
            throw new IOException("Invalid auxiliary plan capacities or reservation", failure);
        }
    }

    static byte[] snapshot(Path directory) throws IOException {
        Path path = directory.resolve("auxiliary.properties");
        if (!Files.exists(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            return null;
        }
        if (!Files.isRegularFile(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)
                || Files.size(path) > 16384) {
            throw new IOException("Auxiliary plan must be a bounded regular file");
        }
        try (var input = Files.newInputStream(path, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
            byte[] snapshot = input.readNBytes(16385);
            if (snapshot.length > 16384) {
                throw new IOException("Auxiliary plan exceeds its snapshot bound");
            }
            return snapshot;
        }
    }

    private Stage2AuxiliaryPlan(byte[] snapshot, Properties campaign) throws IOException {
        sha256 = Stage2CampaignPlan.sha256(snapshot);
        Properties values =
                new Properties() {
                    @Override
                    public synchronized Object put(Object key, Object value) {
                        if (containsKey(key)) {
                            throw new IllegalArgumentException("Duplicate auxiliary field: " + key);
                        }
                        return super.put(key, value);
                    }
                };
        values.load(
                new java.io.StringReader(
                        new String(snapshot, java.nio.charset.StandardCharsets.UTF_8)));
        Set<String> expected =
                new HashSet<>(
                        Set.of(
                                "runOverheadSeconds",
                                "storageAndCleanupSeconds",
                                "otherPreparationSeconds",
                                "serializedSeconds",
                                "sustainedSeconds"));
        for (String name : NAMES) {
            for (String key : LIMITS) {
                expected.add(name + "." + key);
            }
            for (String key : List.of("writeAttempts", "writeBytes", "readBytes")) {
                expected.add(name + "." + key);
            }
        }
        if (!values.stringPropertyNames().equals(expected)) {
            throw new IOException(
                    "Auxiliary plan fields differ from the complete phase reservations");
        }
        long overhead = positive(values, "runOverheadSeconds");
        long storage = positive(values, "storageAndCleanupSeconds");
        long preparation = positive(values, "otherPreparationSeconds");
        var planned = new java.util.ArrayList<Phase>();
        for (int index = 0; index < NAMES.size(); index++) {
            String name = NAMES.get(index);
            long seconds = positive(values, name + "Seconds");
            if ((index == 0 && (seconds < 30 || seconds > 300))
                    || (index == 1 && (seconds < 600 || seconds > 3600))) {
                throw new IOException(
                        "Auxiliary observation duration is outside its declared phase");
            }
            Stage2RunLimits limits =
                    new Stage2RunLimits(
                            Math.toIntExact(positive(values, name + ".inventoryEntries")),
                            positive(values, name + ".inventoryBytes"),
                            Math.toIntExact(positive(values, name + ".stagedEntries")),
                            positive(values, name + ".stagedBytes"),
                            positive(values, name + ".workBytes"),
                            positive(values, name + ".checkpointTimeoutMillis"),
                            positive(values, name + ".drainMillis"));
            long read = Math.multiplyExact(limits.inventoryEntries, 2048L);
            long attempts = positive(values, name + ".writeAttempts");
            long writeBytes = positive(values, name + ".writeBytes");
            long readBytes = positive(values, name + ".readBytes");
            long sampleBytes =
                    index == 0
                            ? BigtableStage2Probe.ORDINARY_SAMPLE_BYTES
                            : BigtableStage2Probe.SUSTAINED_SAMPLE_BYTES;
            if (attempts < Math.multiplyExact(limits.inventoryEntries, 4L)
                    || writeBytes < Math.multiplyExact(read, 4)
                    || readBytes < read
                    || limits.workBytes
                            < Math.addExact(
                                    Math.multiplyExact(limits.inventoryEntries, 64L), sampleBytes)
                    || Math.multiplyExact(overhead, 1000)
                            < Math.addExact(limits.checkpointTimeoutMillis, limits.drainMillis)) {
                throw new IOException(
                        "Auxiliary reservations do not cover inventory, samples and drain");
            }
            long runSeconds = Math.addExact(Math.addExact(WARMUP_SECONDS, seconds), overhead);
            long boundSeconds = Math.addExact(runSeconds, storage);
            preparation = Math.addExact(preparation, boundSeconds);
            planned.add(
                    new Phase(
                            name,
                            seconds,
                            runSeconds,
                            boundSeconds,
                            limits,
                            attempts,
                            writeBytes,
                            readBytes));
        }
        if (preparation > positive(campaign, "campaignOverheadSeconds")) {
            throw new IOException(
                    "Auxiliary phases and preparation exceed the campaign overhead reservation");
        }
        phases = List.copyOf(planned);
    }

    private static long positive(Properties values, String key) throws IOException {
        try {
            long value = Long.parseLong(values.getProperty(key));
            if (value <= 0) {
                throw new NumberFormatException();
            }
            return value;
        } catch (NumberFormatException failure) {
            throw new IOException("Auxiliary reservation must be positive: " + key, failure);
        }
    }

    static final class Phase {
        final String name;
        final long measurementSeconds;
        final long runSeconds;
        final long boundSeconds;
        final Stage2RunLimits limits;
        final long writeAttempts;
        final long writeBytes;
        final long readBytes;

        Phase(
                String name,
                long measurementSeconds,
                long runSeconds,
                long boundSeconds,
                Stage2RunLimits limits,
                long writeAttempts,
                long writeBytes,
                long readBytes) {
            this.name = name;
            this.measurementSeconds = measurementSeconds;
            this.runSeconds = runSeconds;
            this.boundSeconds = boundSeconds;
            this.limits = limits;
            this.writeAttempts = writeAttempts;
            this.writeBytes = writeBytes;
            this.readBytes = readBytes;
        }

        String table() {
            return "stage2-" + name;
        }
    }
}
