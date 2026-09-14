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
import java.io.StringReader;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/** Offline reservation arithmetic and exact table inventory for a retained trial campaign. */
final class Stage2CampaignPlan {
    private static final Set<String> FIELDS =
            Set.of(
                    "project",
                    "instance",
                    "gceZone",
                    "gceMachineType",
                    "jvmFlags",
                    "sourceSha",
                    "runtimeSha256",
                    "leaseLimitSeconds",
                    "runOverheadSeconds",
                    "leaseOverheadSeconds",
                    "campaignOverheadSeconds",
                    "hostMicrousdPerHour",
                    "otherCostMicrousd",
                    "costCeilingMicrousd",
                    "maxWriteAttemptsPerRun",
                    "maxWriteBytesPerRun",
                    "maxReadBytesPerRun",
                    "maxPhysicalStorageBytes");
    private static final long OWNER_CEILING_MICROUSD = 20_000_000;

    private Stage2CampaignPlan() {}

    static void write(Path configuration, Path limitsFile, Path output) throws IOException {
        byte[] configurationBytes = Files.readAllBytes(configuration);
        byte[] limitsBytes = Files.readAllBytes(limitsFile);
        Properties settings = read(configurationBytes);
        Stage2RunLimits limits = Stage2RunLimits.read(limitsBytes);
        long ceiling = positive(settings, "costCeilingMicrousd");
        if (ceiling > OWNER_CEILING_MICROUSD) {
            throw new IllegalArgumentException("Campaign exceeds the owner's USD 20 ceiling");
        }
        if (!settings.getProperty("gceZone").startsWith("us-central1-")) {
            throw new IllegalArgumentException("Campaign compute must be in us-central1");
        }
        requireHash(settings, "sourceSha", 40);
        requireHash(settings, "runtimeSha256", 64);
        long runOverhead = positive(settings, "runOverheadSeconds");
        long minimumOverhead =
                Math.addExact(
                        ceilSeconds(limits.drainMillis),
                        ceilSeconds(limits.checkpointTimeoutMillis));
        if (runOverhead < minimumOverhead) {
            throw new IllegalArgumentException(
                    "runOverheadSeconds must cover drainMillis and checkpointTimeoutMillis");
        }
        long leaseOverhead = positive(settings, "leaseOverheadSeconds");
        long campaignOverhead = positive(settings, "campaignOverheadSeconds");
        long leaseLimit = positive(settings, "leaseLimitSeconds");
        long rate = positive(settings, "hostMicrousdPerHour");
        long otherCost = positive(settings, "otherCostMicrousd");
        for (String key :
                List.of(
                        "maxWriteAttemptsPerRun",
                        "maxWriteBytesPerRun",
                        "maxReadBytesPerRun",
                        "maxPhysicalStorageBytes")) {
            positive(settings, key);
        }
        List<Stage2AssessmentPlan.Run> runs = Stage2AssessmentPlan.runs();
        List<String> leases = new ArrayList<>();
        leases.add(
                "lease,cell,firstRun,lastRun,tablesIncludingSample,admissionSeconds,"
                        + "hostBoundSeconds,hostReservationMicrousd");
        long totalSeconds = Math.max(60, campaignOverhead);
        long hostReservation = hostCost(totalSeconds, rate);
        for (int first = 0; first < runs.size(); first += 6) {
            Stage2AssessmentPlan.Cell cell = runs.get(first).cell;
            long admission = 6L * (cell.warmupSeconds() + cell.measurementSeconds());
            long bound =
                    Math.max(
                            60,
                            Math.addExact(
                                    admission,
                                    Math.addExact(
                                            Math.multiplyExact(6, runOverhead), leaseOverhead)));
            if (bound > leaseLimit) {
                throw new IllegalArgumentException(
                        "Lease does not fit leaseLimitSeconds: " + cell.id());
            }
            long reservation = hostCost(bound, rate);
            totalSeconds = Math.addExact(totalSeconds, bound);
            hostReservation = Math.addExact(hostReservation, reservation);
            leases.add(
                    String.format(
                            Locale.ROOT,
                            "%d,%s,%d,%d,7,%d,%d,%d",
                            first / 6 + 1,
                            cell.id(),
                            first + 1,
                            first + 6,
                            admission,
                            bound,
                            reservation));
        }
        long totalReservation = Math.addExact(hostReservation, otherCost);
        if (totalReservation > ceiling) {
            throw new IllegalArgumentException(
                    "Full campaign reservation exceeds costCeilingMicrousd");
        }
        // Validate all inputs and the complete reservation before creating any output.
        Files.createDirectory(output);
        Files.write(
                output.resolve("inputs.properties"),
                configurationBytes,
                StandardOpenOption.CREATE_NEW);
        Files.write(
                output.resolve("limits.properties"), limitsBytes, StandardOpenOption.CREATE_NEW);
        Stage2AssessmentPlan.write(output.resolve("matrix.csv"));
        Files.write(
                output.resolve("leases.csv"),
                leases,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE_NEW);
        Properties summary = new Properties();
        summary.setProperty("state", "PREPARATION_ONLY");
        summary.setProperty("pricingCondition", "VERIFIED_BIGTABLE_FREE_TRIAL_REQUIRED");
        summary.setProperty("leases", "108");
        summary.setProperty("runs", "648");
        summary.setProperty("maximumTablesIncludingSample", "7");
        summary.setProperty("hostBoundSeconds", Long.toString(totalSeconds));
        summary.setProperty("hostReservationMicrousd", Long.toString(hostReservation));
        summary.setProperty("otherCostMicrousd", Long.toString(otherCost));
        summary.setProperty("totalReservationMicrousd", Long.toString(totalReservation));
        summary.setProperty("costCeilingMicrousd", Long.toString(ceiling));
        for (String file :
                List.of("inputs.properties", "limits.properties", "matrix.csv", "leases.csv")) {
            summary.setProperty(file + ".sha256", sha256(Files.readAllBytes(output.resolve(file))));
        }
        // Written last: a partial directory has no completed plan manifest.
        try (var stream =
                Files.newOutputStream(
                        output.resolve("campaign.properties"), StandardOpenOption.CREATE_NEW)) {
            summary.store(stream, "Offline plan; no resource authorization or lifecycle execution");
        }
    }

    private static Properties read(byte[] bytes) throws IOException {
        Properties settings =
                new Properties() {
                    @Override
                    public synchronized Object put(Object key, Object value) {
                        if (containsKey(key)) {
                            throw new IllegalArgumentException("Duplicate campaign field: " + key);
                        }
                        return super.put(key, value);
                    }
                };
        settings.load(new StringReader(new String(bytes, StandardCharsets.UTF_8)));
        if (!settings.stringPropertyNames().equals(FIELDS)) {
            throw new IllegalArgumentException("Campaign fields must be exactly " + FIELDS);
        }
        for (String key : FIELDS) {
            if (settings.getProperty(key).isBlank()) {
                throw new IllegalArgumentException("Blank campaign field: " + key);
            }
        }
        return settings;
    }

    private static long positive(Properties settings, String key) {
        long value = Long.parseLong(settings.getProperty(key));
        if (value <= 0) {
            throw new IllegalArgumentException(key + " must be positive");
        }
        return value;
    }

    private static void requireHash(Properties settings, String key, int length) {
        if (!settings.getProperty(key).matches("[0-9a-f]{" + length + "}")) {
            throw new IllegalArgumentException(
                    key + " must be a full lowercase hexadecimal digest");
        }
    }

    private static long ceilSeconds(long millis) {
        return Math.addExact(millis / 1000, millis % 1000 == 0 ? 0 : 1);
    }

    private static long hostCost(long seconds, long rate) {
        long product = Math.multiplyExact(seconds, rate);
        return Math.addExact(product / 3600, product % 3600 == 0 ? 0 : 1);
    }

    private static String sha256(byte[] bytes) {
        try {
            return String.format(
                    Locale.ROOT,
                    "%064x",
                    new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(bytes)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
