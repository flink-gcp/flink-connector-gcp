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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Stage2TrialResourcesTest {
    @TempDir Path directory;
    private static final String ROOT = "projects/example-project/instances/example-trial";
    private static final String OWNER = "a".repeat(32);
    private final Instant created = Instant.now().minusSeconds(60);

    @Test
    void adoptsOneTrialAndCreatesOnlySixFreshRawMarkerTables() throws Exception {
        Stage2CampaignJournal journal = journal();
        FakeApi api = new FakeApi();
        Stage2TrialResources resources = new Stage2TrialResources(journal, api);
        resources.adopt();
        Stage2CampaignTestPlan.startWithSimulatedSupervisor(
                journal,
                OWNER,
                ProcessHandle.current().pid(),
                ProcessHandle.current().info().startInstant().orElseThrow().toString(),
                System.currentTimeMillis());
        resources.createCell(0);
        assertThat(api.tables).hasSize(7).contains("weather-data");
        assertThat(api.operationReads).isEqualTo(2);
        assertThat(api.mutations).hasSize(8);
        assertThat(api.mutations.get(0)).isEqualTo("PATCH " + ROOT + "?updateMask=labels");
        assertThat(api.profile.path("singleClusterRouting").path("clusterId").asText())
                .isEqualTo("trial-cluster");
        assertThat(
                        api.profile
                                .path("singleClusterRouting")
                                .path("allowTransactionalWrites")
                                .asBoolean())
                .isTrue();
        for (int index = 0; index < 6; index++) {
            var run = Stage2AssessmentPlan.runs().get(index);
            journal.claim(run.table(), 999_999_990L + index, "worker-" + index);
            journal.finish(run.table(), 999_999_990L + index, "worker-" + index, true);
        }
        resources.deleteCell(0);
        assertThat(api.tables).containsExactly("weather-data");
        journal.cellCleaned("f".repeat(64));
        resources.createCell(1);
        assertThat(api.tables).hasSize(7);
        assertThat(api.mutations).noneMatch(value -> value.equals("DELETE " + ROOT));
    }

    @Test
    void foreignOwnershipAndChangedCreationIdentityPreventDeletion() throws Exception {
        Stage2CampaignJournal journal = journal();
        FakeApi api = new FakeApi();
        Stage2TrialResources resources = new Stage2TrialResources(journal, api);
        api.instance.putObject("labels").put("stage2-owner", "b".repeat(32));
        assertThatThrownBy(resources::adopt).hasMessageContaining("already has an owner");
        assertThatThrownBy(resources::deleteOwnedAndVerifyAbsent).hasMessageContaining("foreign");
        api.instance.putObject("labels").put("stage2-owner", OWNER);
        api.instance.put("createTime", created.plusSeconds(1).toString());
        assertThatThrownBy(resources::deleteOwnedAndVerifyAbsent).hasMessageContaining("foreign");
        assertThat(api.mutations).isEmpty();
    }

    @Test
    void wrongZoneExtraTablesAndMissingPermissionsCannotAdopt() throws Exception {
        Stage2CampaignJournal journal = journal();
        FakeApi api = new FakeApi();
        Stage2TrialResources resources = new Stage2TrialResources(journal, api);
        api.cluster.put("location", "projects/example-project/locations/asia-northeast1-b");
        assertThatThrownBy(resources::adopt).hasMessageContaining("same-zone SSD");
        api.cluster.put("location", "projects/example-project/locations/us-central1-b");
        api.tables.add("foreign-table");
        assertThatThrownBy(resources::adopt).hasMessageContaining("only weather-data");
        assertThat(api.mutations).isEmpty();
        Stage2TrialResources denied =
                new Stage2TrialResources(
                        journal,
                        (method, resource, body) -> {
                            throw new IOException("HTTP 403");
                        });
        assertThatThrownBy(denied::deleteOwnedAndVerifyAbsent).hasMessageContaining("403");
    }

    @Test
    void finalCleanupRequiresBothGetAndListAbsenceAndCanHandleAnAlreadyMissingInstance()
            throws Exception {
        Stage2CampaignJournal journal = journal();
        FakeApi api = new FakeApi();
        api.instance.putObject("labels").put("stage2-owner", OWNER);
        api.staleList = true;
        Stage2TrialResources resources = new Stage2TrialResources(journal, api);
        assertThatThrownBy(resources::deleteOwnedAndVerifyAbsent)
                .hasMessageContaining("independent list");
        assertThat(api.mutations).containsExactly("DELETE " + ROOT);
        api.staleList = false;
        resources.deleteOwnedAndVerifyAbsent();
        assertThat(api.mutations).containsExactly("DELETE " + ROOT);
    }

    @Test
    void pendingOwnerLabelOperationThatFailsCannotPublishSuccessfulAdoption() throws Exception {
        Stage2CampaignJournal journal = journal();
        FakeApi api = new FakeApi();
        api.failedOperation = true;
        Stage2TrialResources resources = new Stage2TrialResources(journal, api);
        assertThatThrownBy(resources::adopt).hasMessageContaining("owner-label operation failed");
        assertThat(api.profile).isNull();
        assertThat(api.operationReads).isEqualTo(2);
        assertThat(api.instance.path("labels").has("stage2-owner")).isFalse();
        assertThat(journal.directory.resolve("adoption.operation.json")).exists();
    }

    @Test
    void partialClusterInventoryCannotBeAdoptedOrConsideredOwned() throws Exception {
        Stage2CampaignJournal journal = journal();
        FakeApi api = new FakeApi();
        api.failedLocation = true;
        Stage2TrialResources resources = new Stage2TrialResources(journal, api);
        assertThatThrownBy(resources::adopt).hasMessageContaining("exactly one cluster");
        assertThat(api.mutations).isEmpty();
        api.instance.putObject("labels").put("stage2-owner", OWNER);
        assertThatThrownBy(resources::verifyOwned).hasMessageContaining("exactly one cluster");
    }

    @Test
    void responseBodyCancelsBeforeRetainingMoreThanItsCap() {
        Stage2TrialResources.BoundedBody body = new Stage2TrialResources.BoundedBody();
        AtomicBoolean cancelled = new AtomicBoolean();
        body.onSubscribe(
                new java.util.concurrent.Flow.Subscription() {
                    @Override
                    public void request(long count) {}

                    @Override
                    public void cancel() {
                        cancelled.set(true);
                    }
                });
        body.onNext(List.of(ByteBuffer.allocate(1024 * 1024)));
        body.onNext(List.of(ByteBuffer.allocate(1)));
        assertThat(cancelled).isTrue();
        assertThatThrownBy(() -> body.getBody().toCompletableFuture().join())
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining("byte cap");
    }

    @Test
    void pollingBeforeActivationPreservesTheAdoptedTrial() throws Exception {
        Stage2CampaignJournal journal = journal();
        FakeApi api = new FakeApi();
        Stage2TrialResources resources = new Stage2TrialResources(journal, api);
        resources.adopt();
        var mutations = List.copyOf(api.mutations);
        Stage2CampaignSupervisor supervisor =
                new Stage2CampaignSupervisor(
                        journal,
                        resources,
                        (pid, start) -> {
                            throw new AssertionError("No worker exists");
                        });
        assertThatThrownBy(supervisor::tick).hasMessageContaining("requires campaign activation");
        assertThat(api.mutations).isEqualTo(mutations);
        assertThat(journal.directory.resolve("stop")).doesNotExist();
        Stage2CampaignTestPlan.startWithSimulatedSupervisor(
                journal,
                OWNER,
                ProcessHandle.current().pid(),
                ProcessHandle.current().info().startInstant().orElseThrow().toString(),
                System.currentTimeMillis());
        assertThat(supervisor.tick()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"/clusters", "/tables", "/appProfiles"})
    void disappearanceDuringInventoryProducesAnActionableFailure(String suffix) throws Exception {
        Stage2CampaignJournal journal = journal();
        FakeApi delegate = new FakeApi();
        Stage2TrialResources resources =
                new Stage2TrialResources(
                        journal,
                        (method, resource, body) ->
                                resource.equals(ROOT + suffix)
                                        ? null
                                        : delegate.call(method, resource, body));
        assertThatThrownBy(resources::adopt)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("disappeared during inventory");
        assertThat(delegate.mutations).isEmpty();
    }

    private Stage2CampaignJournal journal() throws Exception {
        Path campaign = Stage2CampaignTestPlan.write(directory);
        Files.writeString(
                campaign.resolve("trial.properties"),
                "owner="
                        + OWNER
                        + "\ninstanceCreateTime="
                        + created
                        + "\nfreeTrialConfirmedAt="
                        + Instant.now().minusSeconds(1)
                        + "\nfreeTrialExpiresAt="
                        + Instant.now().plusSeconds(10 * 24 * 3600)
                        + "\n");
        return new Stage2CampaignJournal(campaign);
    }

    private final class FakeApi implements Stage2TrialResources.Api {
        private final ObjectMapper mapper = new ObjectMapper();
        private final ObjectNode instance = mapper.createObjectNode();
        private final ObjectNode cluster = mapper.createObjectNode();
        private final Set<String> tables = new HashSet<>(Set.of("weather-data"));
        private final List<String> mutations = new ArrayList<>();
        private JsonNode profile;
        private boolean absent;
        private boolean staleList;
        private boolean failedOperation;
        private boolean failedLocation;
        private JsonNode pendingLabels;
        private int operationReads;
        private final String operationName =
                "operations/projects/example-project/locations/us-central1-b/operations/owner";

        FakeApi() {
            instance.put("name", ROOT).put("createTime", created.toString()).put("state", "READY");
            instance.putObject("labels");
            cluster.put("name", ROOT + "/clusters/trial-cluster")
                    .put("location", "projects/example-project/locations/us-central1-b")
                    .put("state", "READY")
                    .put("defaultStorageType", "SSD")
                    .put("serveNodes", 1);
        }

        @Override
        public JsonNode call(String method, String resource, JsonNode body) {
            if (!method.equals("GET")) {
                mutations.add(method + " " + resource);
            }
            if (method.equals("GET") && resource.equals(ROOT)) {
                return absent ? null : instance.deepCopy();
            }
            if (resource.equals(ROOT + "/clusters")) {
                var response = mapper.createObjectNode();
                response.putArray("clusters").add(cluster.deepCopy());
                if (failedLocation) {
                    response.putArray("failedLocations").add("us-central1-c");
                }
                return response;
            }
            if (method.equals("GET") && resource.equals(ROOT + "/appProfiles")) {
                var response = mapper.createObjectNode();
                response.putArray("appProfiles")
                        .addObject()
                        .put("name", ROOT + "/appProfiles/default");
                return response;
            }
            if (method.equals("PATCH")) {
                assertThat(body.fieldNames())
                        .toIterable()
                        .containsExactlyInAnyOrder("name", "labels");
                pendingLabels = body.path("labels").deepCopy();
                return mapper.createObjectNode().put("name", operationName);
            }
            if (method.equals("GET") && resource.equals(operationName)) {
                assertThat(profile).isNull();
                operationReads++;
                var operation = mapper.createObjectNode().put("name", operationName);
                if (operationReads == 2) {
                    operation.put("done", true);
                    if (failedOperation) {
                        operation.putObject("error").put("code", 7);
                    } else {
                        instance.set("labels", pendingLabels);
                    }
                }
                return operation;
            }
            if (method.equals("POST") && resource.contains("/appProfiles?")) {
                assertThat(operationReads).isEqualTo(2);
                profile = body.deepCopy();
                return profile;
            }
            if (resource.equals(ROOT + "/tables")) {
                if (method.equals("POST")) {
                    JsonNode families = body.path("table").path("columnFamilies");
                    assertThat(families.fieldNames())
                            .toIterable()
                            .containsExactlyInAnyOrder("cf", "flink_commit");
                    assertThat(families.path("flink_commit").size()).isZero();
                    assertThat(tables.add(body.path("tableId").asText())).isTrue();
                    return mapper.createObjectNode();
                }
                var response = mapper.createObjectNode();
                var values = response.putArray("tables");
                tables.forEach(table -> values.addObject().put("name", ROOT + "/tables/" + table));
                return response;
            }
            if (method.equals("DELETE") && resource.startsWith(ROOT + "/tables/")) {
                assertThat(tables.remove(resource.substring((ROOT + "/tables/").length())))
                        .isTrue();
                return mapper.createObjectNode();
            }
            if (method.equals("DELETE") && resource.equals(ROOT)) {
                absent = true;
                return mapper.createObjectNode();
            }
            if (resource.equals("projects/example-project/instances")) {
                var response = mapper.createObjectNode();
                var values = response.putArray("instances");
                if (!absent || staleList) {
                    values.add(instance.deepCopy());
                }
                return response;
            }
            throw new AssertionError(
                    "Unexpected control-plane request: " + method + " " + resource);
        }
    }
}
