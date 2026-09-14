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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2CampaignPlanTest {
    @TempDir Path directory;

    @Test
    void cliKeepsAllPairedRunsAndIncludesEveryOverheadInTheReservation() throws Exception {
        Path inputs = inputs();
        Path limits = limits();
        Path output = directory.resolve("campaign");
        BigtableStage2Probe.main(
                new String[] {
                    "plan-campaign", inputs.toString(), limits.toString(), output.toString()
                });
        List<String> leases = Files.readAllLines(output.resolve("leases.csv"));
        assertThat(leases).hasSize(109);
        // Synthetic rate: one micro-USD per host second, no external pricing assumption.
        assertThat(leases.get(1)).isEqualTo("1,b1024-even-p1-i1-c1,1,6,7,240,2100,2100");
        assertThat(leases.get(108))
                .isEqualTo("108,b65536-hot-p16-i16-c60,643,648,7,1440,3300,3300");
        List<String> matrix = Files.readAllLines(output.resolve("matrix.csv"));
        assertThat(matrix).hasSize(649);
        assertThat(
                        matrix.subList(1, matrix.size()).stream()
                                .map(line -> line.split(",")[2])
                                .collect(Collectors.toList()))
                .containsExactlyElementsOf(
                        Stage2AssessmentPlan.runs().stream()
                                .map(Stage2AssessmentPlan.Run::table)
                                .collect(Collectors.toList()));
        for (String lease : leases.subList(1, leases.size())) {
            String[] fields = lease.split(",");
            for (int run = Integer.parseInt(fields[2]); run <= Integer.parseInt(fields[3]); run++) {
                assertThat(matrix.get(run).split(",")[1]).isEqualTo(fields[1]);
            }
        }
        Properties summary = summary(output);
        // 69,120 admission + 648 * 300 per-run + 108 * 60 per-lease + 600 campaign.
        assertThat(summary.getProperty("hostBoundSeconds")).isEqualTo("270600");
        assertThat(summary.getProperty("hostReservationMicrousd")).isEqualTo("270600");
        assertThat(summary.getProperty("totalReservationMicrousd")).isEqualTo("1270600");
        assertThat(summary.getProperty("state")).isEqualTo("PREPARATION_ONLY");
        assertThat(summary.getProperty("pricingCondition"))
                .isEqualTo("VERIFIED_BIGTABLE_FREE_TRIAL_REQUIRED");
        assertThat(Files.readAllBytes(output.resolve("inputs.properties")))
                .isEqualTo(Files.readAllBytes(inputs));
        assertThat(Files.readAllBytes(output.resolve("limits.properties")))
                .isEqualTo(Files.readAllBytes(limits));
        for (String file :
                List.of("inputs.properties", "limits.properties", "matrix.csv", "leases.csv")) {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(Files.readAllBytes(output.resolve(file)));
            StringBuilder hexadecimal = new StringBuilder();
            for (byte value : digest) {
                hexadecimal.append(Character.forDigit((value & 255) >>> 4, 16));
                hexadecimal.append(Character.forDigit(value & 15, 16));
            }
            assertThat(summary.getProperty(file + ".sha256")).isEqualTo(hexadecimal.toString());
        }
        assertThatThrownBy(() -> Stage2CampaignPlan.write(inputs, limits, output))
                .isInstanceOf(FileAlreadyExistsException.class);
        assertThat(Files.readAllLines(output.resolve("leases.csv"))).isEqualTo(leases);
        try (var files = Files.list(output)) {
            assertThat(files.count()).isEqualTo(5);
        }
    }

    @Test
    void fractionalMicrodollarsRoundUpForEveryLeaseAndTheCampaignOverhead() throws Exception {
        Path inputs = inputs();
        replace(inputs, "hostMicrousdPerHour=3600", "hostMicrousdPerHour=1");
        Path output = directory.resolve("campaign");
        Stage2CampaignPlan.write(inputs, limits(), output);
        assertThat(summary(output).getProperty("hostReservationMicrousd")).isEqualTo("109");
    }

    @ParameterizedTest
    @CsvSource({
        "leaseLimitSeconds=3600, leaseLimitSeconds=3299, Lease does not fit",
        "runOverheadSeconds=300, runOverheadSeconds=179, must cover",
        "costCeilingMicrousd=20000000, costCeilingMicrousd=21000000, USD 20",
        "costCeilingMicrousd=20000000, costCeilingMicrousd=1270599, Full campaign reservation",
        "hostMicrousdPerHour=3600, hostMicrousdPerHour=0, must be positive",
        "otherCostMicrousd=1000000, otherCostMicrousd=0, must be positive",
        "maxReadBytesPerRun=1073741824, maxReadBytesPerRun=-1, must be positive",
        "gceZone=us-central1-b, gceZone=asia-northeast1-a, us-central1",
        "project=example-project, project=, Blank campaign field",
        "project=example-project, , Campaign fields must be exactly",
        "project=example-project, extra=value, Campaign fields must be exactly",
        "hostMicrousdPerHour=3600, hostMicrousdPerHour=not-a-number, For input string"
    })
    void invalidOrUnderfundedPlansLeaveNoOutput(String before, String after, String failure)
            throws Exception {
        Path inputs = inputs();
        replace(inputs, before, after);
        Path limits = limits();
        Path output = directory.resolve("campaign");
        assertThatThrownBy(() -> Stage2CampaignPlan.write(inputs, limits, output))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(failure);
        assertThat(output).doesNotExist();
    }

    @Test
    void arithmeticOverflowAndDuplicateBudgetFieldsCannotProduceAPlan() throws Exception {
        Path inputs = inputs();
        Path limits = limits();
        Path output = directory.resolve("campaign");
        replace(inputs, "hostMicrousdPerHour=3600", "hostMicrousdPerHour=" + Long.MAX_VALUE);
        assertThatThrownBy(() -> Stage2CampaignPlan.write(inputs, limits, output))
                .isInstanceOf(ArithmeticException.class);
        assertThat(output).doesNotExist();
        Files.writeString(inputs, Files.readString(inputs) + "costCeilingMicrousd=1\n");
        assertThatThrownBy(() -> Stage2CampaignPlan.write(inputs, limits, output))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate campaign field");
        assertThat(output).doesNotExist();
    }

    @ParameterizedTest
    @ValueSource(strings = {"sourceSha", "runtimeSha256"})
    void incompleteArtifactIdentitiesLeaveNoOutput(String key) throws Exception {
        Path inputs = inputs();
        Files.writeString(
                inputs, Files.readString(inputs).replaceAll("(?m)^" + key + "=.*$", key + "=abc"));
        Path limits = limits();
        Path output = directory.resolve("campaign");
        assertThatThrownBy(() -> Stage2CampaignPlan.write(inputs, limits, output))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(key);
        assertThat(output).doesNotExist();
    }

    @Test
    void malformedLimitsCannotProduceACampaignSnapshot() throws Exception {
        Path inputs = inputs();
        Path limits = limits();
        Files.writeString(limits, "drainMillis=120000\n");
        Path output = directory.resolve("campaign");
        assertThatThrownBy(() -> Stage2CampaignPlan.write(inputs, limits, output))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("exactly the seven");
        assertThat(output).doesNotExist();
    }

    @Test
    void exactLeaseAndReservationBoundsAreAccepted() throws Exception {
        Path inputs = inputs();
        replace(inputs, "leaseLimitSeconds=3600", "leaseLimitSeconds=3300");
        replace(inputs, "costCeilingMicrousd=20000000", "costCeilingMicrousd=1270600");
        Path output = directory.resolve("campaign");
        Stage2CampaignPlan.write(inputs, limits(), output);
        Properties summary = summary(output);
        assertThat(summary.getProperty("totalReservationMicrousd"))
                .isEqualTo(summary.getProperty("costCeilingMicrousd"));
        assertThat(Files.readAllLines(output.resolve("leases.csv")).get(108).split(",")[6])
                .isEqualTo("3300");
    }

    private Path inputs() throws Exception {
        return Files.writeString(
                directory.resolve("inputs.properties"),
                "project=example-project\ninstance=example-trial\ngceZone=us-central1-b\n"
                        + "gceMachineType=example-machine\njvmFlags=-Xmx2g\nsourceSha="
                        + "a".repeat(40)
                        + "\nruntimeSha256="
                        + "b".repeat(64)
                        + "\nleaseLimitSeconds=3600\nrunOverheadSeconds=300\nleaseOverheadSeconds=60\n"
                        + "campaignOverheadSeconds=600\nhostMicrousdPerHour=3600\notherCostMicrousd=1000000\n"
                        + "costCeilingMicrousd=20000000\nmaxWriteAttemptsPerRun=1000000\n"
                        + "maxWriteBytesPerRun=1073741824\nmaxReadBytesPerRun=1073741824\n"
                        + "maxPhysicalStorageBytes=1073741824\n");
    }

    private Path limits() throws Exception {
        return Files.writeString(
                directory.resolve("limits.properties"),
                "inventoryEntries=1000000\ninventoryBytes=67108864\nstagedEntries=100000\n"
                        + "stagedBytes=67108864\nworkBytes=2147483648\n"
                        + "checkpointTimeoutMillis=60000\ndrainMillis=120000\n");
    }

    private static void replace(Path path, String before, String after) throws Exception {
        Files.writeString(path, Files.readString(path).replace(before, after == null ? "" : after));
    }

    private static Properties summary(Path output) throws Exception {
        Properties result = new Properties();
        try (var input = Files.newInputStream(output.resolve("campaign.properties"))) {
            result.load(input);
        }
        return result;
    }
}
