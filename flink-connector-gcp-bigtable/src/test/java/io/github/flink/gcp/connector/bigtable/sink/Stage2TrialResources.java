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

import com.google.auth.oauth2.GoogleCredentials;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Exact-instance control plane; there is deliberately no instance creation or upgrade operation.
 */
final class Stage2TrialResources implements Stage2CampaignSupervisor.Resources {
    interface Api {
        JsonNode call(String method, String resource, JsonNode body) throws Exception;
    }

    private final Stage2CampaignJournal journal;
    private final Api api;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String root;
    private final String owner;
    private final String createTime;
    private final Instant expires;

    Stage2TrialResources(Stage2CampaignJournal journal, Api api) throws IOException {
        this.journal = journal;
        this.api = api;
        Properties proof = new Properties();
        try (var input = Files.newInputStream(journal.directory.resolve("trial.properties"))) {
            proof.load(input);
        }
        if (!proof.stringPropertyNames()
                .equals(
                        Set.of(
                                "owner",
                                "instanceCreateTime",
                                "freeTrialConfirmedAt",
                                "freeTrialExpiresAt"))) {
            throw new IOException("Exact Console trial attestation is required before adoption");
        }
        owner = proof.getProperty("owner");
        createTime = proof.getProperty("instanceCreateTime");
        expires = Instant.parse(proof.getProperty("freeTrialExpiresAt"));
        Instant confirmed = Instant.parse(proof.getProperty("freeTrialConfirmedAt"));
        if (!owner.matches("[a-f0-9]{32}")
                || confirmed.isAfter(Instant.now())
                || !expires.isAfter(confirmed)
                || Instant.parse(createTime).isAfter(confirmed)) {
            throw new IOException("Invalid Console trial attestation");
        }
        root =
                "projects/"
                        + segment(journal.inputs.getProperty("project"))
                        + "/instances/"
                        + segment(journal.inputs.getProperty("instance"));
    }

    String owner() {
        return owner;
    }

    /** Claim only the new trial identified by the operator's Console creation timestamp. */
    void adopt() throws Exception {
        if (!Instant.now()
                .plusSeconds(Math.addExact(journal.hostBoundSeconds(), 600))
                .isBefore(expires)) {
            throw new IOException(
                    "Complete campaign and cleanup do not fit the confirmed trial period");
        }
        JsonNode instance = instance();
        String existingOwner = instance.path("labels").path("stage2-owner").asText("");
        if (!existingOwner.isEmpty()) {
            throw new IOException("Trial already has an owner; adoption cannot be repeated");
        }
        requireCluster();
        if (!tables().equals(Set.of("weather-data"))) {
            throw new IOException("Trial initial table inventory must contain only weather-data");
        }
        JsonNode profiles = api.call("GET", root + "/appProfiles", null);
        rejectNextPage(profiles);
        for (JsonNode profile : profiles.path("appProfiles")) {
            if (!profile.path("name").asText().equals(root + "/appProfiles/default")) {
                throw new IOException("Trial has an unexpected application profile");
            }
        }
        var patch = mapper.createObjectNode();
        patch.put("name", root);
        var labels = patch.putObject("labels");
        instance.path("labels")
                .fields()
                .forEachRemaining(entry -> labels.set(entry.getKey(), entry.getValue()));
        labels.put("stage2-owner", owner);
        Files.writeString(
                journal.directory.resolve("adoption.started"),
                createTime + "\n" + owner,
                java.nio.file.StandardOpenOption.CREATE_NEW);
        Stage2CampaignJournal.force(journal.directory.resolve("adoption.started"));
        Stage2CampaignJournal.force(journal.directory);
        JsonNode operation = api.call("PATCH", root + "?updateMask=labels", patch);
        Files.writeString(
                journal.directory.resolve("adoption.operation.json"),
                mapper.writeValueAsString(operation),
                java.nio.file.StandardOpenOption.CREATE_NEW);
        Stage2CampaignJournal.force(journal.directory.resolve("adoption.operation.json"));
        Stage2CampaignJournal.force(journal.directory);
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(30);
        while (!operation.path("done").asBoolean()) {
            String name = operation.path("name").asText();
            if (!name.startsWith(
                            "operations/projects/"
                                    + segment(journal.inputs.getProperty("project"))
                                    + "/")
                    || System.nanoTime() >= deadline) {
                throw new IOException(
                        "Trial label update did not finish within its operation bound");
            }
            Thread.sleep(200);
            operation = required(api.call("GET", name, null));
        }
        if (operation.has("error")) {
            throw new IOException("Trial owner-label operation failed");
        }
        verifyOwned();
        var profile = mapper.createObjectNode();
        var routing = profile.putObject("singleClusterRouting");
        routing.put("clusterId", requireCluster());
        routing.put("allowTransactionalWrites", true);
        api.call(
                "POST",
                root + "/appProfiles?appProfileId=" + segment(LocalStagedHarness.PROFILE),
                profile);
    }

    private JsonNode instance() throws Exception {
        JsonNode instance = api.call("GET", root, null);
        if (instance == null
                || !root.equals(instance.path("name").asText())
                || !createTime.equals(instance.path("createTime").asText())
                || !"READY".equals(instance.path("state").asText())) {
            throw new IOException("Trial disappeared or its creation identity/state changed");
        }
        if (!Instant.now().plusSeconds(600).isBefore(expires)) {
            throw new IOException("Trial expiry leaves no cleanup reserve");
        }
        return instance;
    }

    private String requireCluster() throws Exception {
        JsonNode response = api.call("GET", root + "/clusters", null);
        rejectNextPage(response);
        JsonNode clusters = response.path("clusters");
        if (clusters.size() != 1 || !response.path("failedLocations").isEmpty()) {
            throw new IOException("Trial requires exactly one cluster");
        }
        JsonNode cluster = clusters.get(0);
        String location = cluster.path("location").asText();
        if (!location.endsWith("/locations/" + journal.inputs.getProperty("gceZone"))
                || !"READY".equals(cluster.path("state").asText())
                || !"SSD".equals(cluster.path("defaultStorageType").asText())
                || cluster.path("serveNodes").asInt() != 1
                || cluster.path("clusterConfig").has("clusterAutoscalingConfig")) {
            throw new IOException(
                    "Trial cluster differs from the fixed one-node, same-zone SSD plan");
        }
        String name = cluster.path("name").asText();
        if (!name.startsWith(root + "/clusters/")
                || name.substring((root + "/clusters/").length()).contains("/")) {
            throw new IOException("Invalid owned cluster name");
        }
        return name.substring((root + "/clusters/").length());
    }

    @Override
    public void verifyOwned() throws Exception {
        if (!owner.equals(instance().path("labels").path("stage2-owner").asText())) {
            throw new IOException("Trial owner differs; preserve the foreign resource");
        }
        requireCluster();
    }

    Set<String> tables() throws Exception {
        JsonNode response = api.call("GET", root + "/tables", null);
        rejectNextPage(response);
        Set<String> result = new HashSet<>();
        for (JsonNode table : response.path("tables")) {
            String name = table.path("name").asText();
            String prefix = root + "/tables/";
            if (!name.startsWith(prefix) || name.substring(prefix.length()).contains("/")) {
                throw new IOException("Unexpected table resource name");
            }
            result.add(name.substring(prefix.length()));
        }
        return result;
    }

    void createCell(int cell) throws Exception {
        verifyOwned();
        if (!tables().equals(Set.of("weather-data"))) {
            throw new IOException("Previous cell tables remain or table inventory is foreign");
        }
        journal.prepareCell(cell);
        for (var run : Stage2AssessmentPlan.runs().subList(cell * 6, cell * 6 + 6)) {
            verifyOwned();
            var request = mapper.createObjectNode();
            request.put("tableId", run.table());
            var families = request.putObject("table").putObject("columnFamilies");
            families.putObject("cf");
            families.putObject(StagedMutationTestSink.MARKER_FAMILY);
            api.call("POST", root + "/tables", request);
        }
        Set<String> expected = new HashSet<>(Set.of("weather-data"));
        Stage2AssessmentPlan.runs()
                .subList(cell * 6, cell * 6 + 6)
                .forEach(run -> expected.add(run.table()));
        if (!tables().equals(expected)) {
            throw new IOException("Created cell table inventory differs from the plan");
        }
        journal.tablesReady();
    }

    void deleteCell(int cell) throws Exception {
        verifyOwned();
        if (!"RETAINING".equals(journal.read().getProperty("phase"))
                || Integer.parseInt(journal.read().getProperty("cell")) != cell) {
            throw new IOException("Cell is not ready for retained-evidence cleanup");
        }
        Set<String> expected = new HashSet<>(Set.of("weather-data"));
        Stage2AssessmentPlan.runs()
                .subList(cell * 6, cell * 6 + 6)
                .forEach(run -> expected.add(run.table()));
        if (!tables().equals(expected)) {
            throw new IOException("Cell table inventory changed before cleanup");
        }
        for (var run : Stage2AssessmentPlan.runs().subList(cell * 6, cell * 6 + 6)) {
            verifyOwned();
            api.call("DELETE", root + "/tables/" + segment(run.table()), null);
        }
        if (!tables().equals(Set.of("weather-data"))) {
            throw new IOException("Owned cell tables remain after deletion");
        }
    }

    @Override
    public void deleteOwnedAndVerifyAbsent() throws Exception {
        JsonNode instance = api.call("GET", root, null);
        if (instance != null) {
            // Cleanup checks identity without requiring a healthy cluster or an unexpired trial.
            if (!root.equals(instance.path("name").asText())
                    || !createTime.equals(instance.path("createTime").asText())
                    || !owner.equals(instance.path("labels").path("stage2-owner").asText())) {
                throw new IOException("Cleanup refuses a foreign trial instance");
            }
            api.call("DELETE", root, null);
        }
        if (api.call("GET", root, null) != null) {
            throw new IOException("Trial instance remains after deletion");
        }
        JsonNode listed =
                api.call(
                        "GET",
                        "projects/" + segment(journal.inputs.getProperty("project")) + "/instances",
                        null);
        rejectNextPage(listed);
        if (listed.path("failedLocations").size() > 0) {
            throw new IOException("Instance absence inventory contains failed locations");
        }
        for (JsonNode value : listed.path("instances")) {
            if (root.equals(value.path("name").asText())) {
                throw new IOException("Deleted trial remains in the independent list");
            }
        }
    }

    private static JsonNode required(JsonNode value) throws IOException {
        if (value == null) {
            throw new IOException("Trial resource disappeared during inventory or operation read");
        }
        return value;
    }

    private static void rejectNextPage(JsonNode value) throws IOException {
        if (!required(value).path("nextPageToken").asText("").isEmpty()) {
            throw new IOException("Trial inventory exceeds a single bounded page");
        }
    }

    private static String segment(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static Api nativeApi() throws IOException {
        GoogleCredentials credentials =
                GoogleCredentials.getApplicationDefault()
                        .createScoped("https://www.googleapis.com/auth/cloud-platform");
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        ObjectMapper mapper = new ObjectMapper();
        return (method, resource, body) -> {
            credentials.refreshIfExpired();
            HttpRequest request =
                    HttpRequest.newBuilder(
                                    URI.create(
                                            "https://bigtableadmin.googleapis.com/v2/" + resource))
                            .header(
                                    "Authorization",
                                    "Bearer " + credentials.getAccessToken().getTokenValue())
                            .header("Content-Type", "application/json")
                            .timeout(Duration.ofSeconds(15))
                            .method(
                                    method,
                                    body == null
                                            ? HttpRequest.BodyPublishers.noBody()
                                            : HttpRequest.BodyPublishers.ofString(
                                                    mapper.writeValueAsString(body)))
                            .build();
            var pending = http.sendAsync(request, info -> new BoundedBody());
            HttpResponse<byte[]> response;
            try {
                response = pending.get(15, java.util.concurrent.TimeUnit.SECONDS);
            } catch (Exception failure) {
                pending.cancel(true);
                throw failure;
            }
            if (response.statusCode() == 404 && method.equals("GET")) {
                return null;
            }
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IOException(
                        "Trial control-plane " + method + " failed: HTTP " + response.statusCode());
            }
            byte[] bytes = response.body();
            return bytes.length == 0 ? mapper.createObjectNode() : mapper.readTree(bytes);
        };
    }

    static final class BoundedBody implements HttpResponse.BodySubscriber<byte[]> {
        private final java.util.concurrent.CompletableFuture<byte[]> result =
                new java.util.concurrent.CompletableFuture<>();
        private final java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        private java.util.concurrent.Flow.Subscription subscription;

        @Override
        public java.util.concurrent.CompletionStage<byte[]> getBody() {
            return result;
        }

        @Override
        public void onSubscribe(java.util.concurrent.Flow.Subscription value) {
            subscription = value;
            subscription.request(1);
        }

        @Override
        public void onNext(java.util.List<java.nio.ByteBuffer> buffers) {
            for (var buffer : buffers) {
                if (buffer.remaining() > 1024 * 1024 - bytes.size()) {
                    subscription.cancel();
                    result.completeExceptionally(
                            new IOException("Trial control-plane response exceeds its byte cap"));
                    return;
                }
                byte[] next = new byte[buffer.remaining()];
                buffer.get(next);
                bytes.write(next, 0, next.length);
            }
            subscription.request(1);
        }

        @Override
        public void onError(Throwable failure) {
            result.completeExceptionally(failure);
        }

        @Override
        public void onComplete() {
            result.complete(bytes.toByteArray());
        }
    }
}
