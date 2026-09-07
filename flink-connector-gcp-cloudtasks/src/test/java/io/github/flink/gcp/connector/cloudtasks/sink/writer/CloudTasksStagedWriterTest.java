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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import com.google.cloud.tasks.v2.AppEngineHttpRequest;
import com.google.cloud.tasks.v2.AppEngineRouting;
import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.OAuthToken;
import com.google.cloud.tasks.v2.OidcToken;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCommittable;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSink;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkBuilder;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagingConfig;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CloudTasksStagedWriterTest {
    private final ManualTimeSource time = new ManualTimeSource();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
    private final CountingRandom random = new CountingRandom();

    @Test
    void observesOnlyWriterOwnedTimeAndClearsOnTransferAndClose() throws Exception {
        var writer = writer(TestSinkConfigs.builder(), new CloudTasksStagingConfig());
        assertThat(metrics.<Long>gaugeValue("oldestStagedTaskAgeMillis")).isEqualTo(-1);
        assertThat(metrics.<Long>gaugeValue("stagedReplayBudgetMillis")).isEqualTo(-1);
        writer.write("first", TestContexts.NO_OP);
        time.sleep(500);
        writer.write("second", TestContexts.NO_OP);
        assertThat(metrics.<Long>gaugeValue("oldestStagedTaskAgeMillis")).isEqualTo(500);
        assertThat(metrics.<Long>gaugeValue("stagedReplayBudgetMillis")).isEqualTo(3_279_500);
        writer.flush(false);
        assertThat(metrics.<Long>gaugeValue("oldestStagedTaskAgeMillis")).isEqualTo(500);
        writer.prepareCommit();
        assertThat(metrics.<Long>gaugeValue("oldestStagedTaskAgeMillis")).isEqualTo(-1);
        assertThat(metrics.<Long>gaugeValue("stagedReplayBudgetMillis")).isEqualTo(-1);
        writer.write("next", TestContexts.NO_OP);
        time.sleep(3_280_000);
        assertThat(metrics.<Long>gaugeValue("stagedReplayBudgetMillis")).isZero();
        writer.close();
        assertThat(metrics.<Long>gaugeValue("stagedReplayBudgetMillis")).isEqualTo(-1);
    }

    @Test
    void randomIdentityIsMintedOncePerAcceptedRecordAndSurvivesOwnershipTransfer()
            throws Exception {
        var writer = writer(TestSinkConfigs.builder(), new CloudTasksStagingConfig());
        writer.write("same", TestContexts.NO_OP);
        time.sleep(500);
        writer.write("same", TestContexts.NO_OP);
        assertThat(metrics.<Integer>gaugeValue("stagedTasks")).isEqualTo(2);
        assertThat(random.calls).isEqualTo(2);
        var emitted = writer.prepareCommit();
        var envelopes = emitted.stream().toList();
        assertThat(envelopes.get(0).parseTask().getName()).endsWith("0".repeat(31) + "1");
        assertThat(envelopes.get(1).parseTask().getName()).endsWith("0".repeat(31) + "2");
        assertThat(envelopes.get(0).getOriginEpochMillis()).isEqualTo(1_000_000);
        assertThat(envelopes.get(1).getOriginEpochMillis()).isEqualTo(1_000_500);
        assertThat(envelopes.get(0).getAuthorizationDeadlineMillis()).isEqualTo(4_280_000);
        assertThat(metrics.<Integer>gaugeValue("stagedTasks")).isZero();
        assertThat(metrics.<Long>gaugeValue("stagedBytes")).isZero();
        assertThat(writer.prepareCommit()).isEmpty();
        writer.write("third", TestContexts.NO_OP);
        writer.close();
        assertThat(emitted).containsExactlyElementsOf(envelopes);
        assertThatThrownBy(emitted::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(writer::prepareCommit).hasMessageContaining("closed");
        assertThat(metrics.counterValue("numRecordsSend")).isZero();
    }

    @Test
    void stableKeysKeepTheExistingSha256IdentityAndSkipRandomGeneration() throws Exception {
        var writer =
                writer(
                        TestSinkConfigs.builder().taskIdExtractor(element -> element),
                        new CloudTasksStagingConfig());
        writer.write("order-1", TestContexts.NO_OP);
        writer.write("order-1", TestContexts.NO_OP);
        var entries = writer.prepareCommit().stream().toList();
        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).parseTask().getName())
                .isEqualTo(TestSinkConfigs.QUEUE_PATH + "/tasks/" + TestSinkConfigs.ORDER_1_DIGEST);
        assertThat(entries.get(1).parseTask().getName())
                .isEqualTo(entries.get(0).parseTask().getName());
        assertThat(random.calls).isZero();
    }

    @Test
    void nullSkipsWithoutIdentityOrCapacityConsumption() throws Exception {
        var writer =
                writer(TestSinkConfigs.builder(TestSinkConfigs.QUEUE, element -> null), caps(1, 1));
        writer.write("skipped", TestContexts.NO_OP);
        assertThat(writer.prepareCommit()).isEmpty();
        assertThat(random.calls).isZero();
        assertThat(metrics.counterValue("recordsSkipped")).isEqualTo(1);
        assertThat(metrics.counterValue("numRecordsSendErrors")).isZero();
    }

    @Test
    @Timeout(5)
    void countOverflowFailsWithoutWaitingAndLeavesTheAcceptedBatchIntact() throws Exception {
        var writer = writer(TestSinkConfigs.builder(), caps(3, 1_000_000));
        for (int i = 0; i < 3; i++) {
            writer.write("secret-payload", TestContexts.NO_OP);
        }
        assertThatThrownBy(() -> writer.write("secret-payload", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxStagedTasks=3")
                .hasMessageContaining("maxStagedBytes=1000000")
                .hasMessageContaining("stagedTasks=3")
                .hasMessageContaining("Shorten the checkpoint interval")
                .hasMessageNotContaining("secret-payload");
        assertThat(writer.prepareCommit()).hasSize(3);
        assertThat(time.getSleptMillis()).isZero();
    }

    @Test
    @Timeout(5)
    void byteCapUsesTheNamedWireSizePlusOverheadAndResetsAfterTransfer() throws Exception {
        Task original = TestSinkConfigs.serializer().serialize("record");
        Task named =
                original.toBuilder()
                        .setName(TestSinkConfigs.QUEUE_PATH + "/tasks/" + "a".repeat(32))
                        .build();
        long accounted = named.getSerializedSize() + 256L;
        var writer = writer(TestSinkConfigs.builder(), caps(10, accounted));
        writer.write("record", TestContexts.NO_OP);
        assertThat(metrics.<Long>gaugeValue("stagedBytes")).isEqualTo(accounted);
        assertThatThrownBy(() -> writer.write("record", TestContexts.NO_OP))
                .hasMessageContaining("additionalBytes=" + accounted);
        Collection<CloudTasksCommittable> first = writer.prepareCommit();
        writer.write("record", TestContexts.NO_OP);
        assertThat(first).hasSize(1);
        assertThat(writer.prepareCommit()).hasSize(1);
    }

    @Test
    void oversizeIsASerializationFailureEvenWhenCapacityIsAlsoExceeded() {
        var writer =
                writer(
                        TestSinkConfigs.builder(
                                TestSinkConfigs.QUEUE,
                                element ->
                                        Task.newBuilder()
                                                .setHttpRequest(
                                                        HttpRequest.newBuilder()
                                                                .setUrl("https://example.com")
                                                                .setHttpMethod(HttpMethod.POST)
                                                                .setBody(
                                                                        ByteString.copyFrom(
                                                                                new byte[100_000])))
                                                .build()),
                        caps(1, 1));
        assertThatThrownBy(() -> writer.write("ignored", TestContexts.NO_OP))
                .hasMessageContaining("serialization failed")
                .hasMessageContaining("100000")
                .hasMessageNotContaining("capacity exceeded");
        assertThat(writer.getStagedTasks()).isZero();
        assertThat(metrics.counterValue("numRecordsSendErrors")).isEqualTo(1);
    }

    @Test
    void preservesResolverAndExtractorValidationWithoutLeakingTheirValues() throws Exception {
        var noQueue =
                writer(
                        CloudTasksSink.<String>builder()
                                .destinationResolver((element, context) -> null)
                                .serializer(TestSinkConfigs.serializer()),
                        new CloudTasksStagingConfig());
        assertThatThrownBy(() -> noQueue.write("x", TestContexts.NO_OP))
                .hasMessageContaining("resolver returned null");
        for (String key : new String[] {null, ""}) {
            var missing =
                    writer(
                            TestSinkConfigs.builder().taskIdExtractor(element -> key),
                            new CloudTasksStagingConfig());
            assertThatThrownBy(() -> missing.write("x", TestContexts.NO_OP))
                    .hasMessageContaining("every record needs a key");
        }
        var throwing =
                writer(
                        TestSinkConfigs.builder()
                                .taskIdExtractor(
                                        element -> {
                                            throw new IllegalArgumentException("secret-key");
                                        }),
                        new CloudTasksStagingConfig());
        assertThatThrownBy(() -> throwing.write("x", TestContexts.NO_OP))
                .hasMessageContaining("extractor failed")
                .hasMessageNotContaining("secret-key")
                .hasNoCause();
        var named =
                writer(
                        TestSinkConfigs.builder(
                                TestSinkConfigs.QUEUE,
                                element -> Task.newBuilder().setName("secret-name").build()),
                        new CloudTasksStagingConfig());
        assertThatThrownBy(() -> named.write("x", TestContexts.NO_OP))
                .hasMessageContaining("already named")
                .hasMessageNotContaining("secret-name");
    }

    @Test
    void rejectsSerializerFailureAndPoisonTargetsBeforeStaging() throws Exception {
        var failure =
                writer(
                        TestSinkConfigs.builder(
                                TestSinkConfigs.QUEUE,
                                element -> {
                                    throw new IOException(
                                            "secret-body Authorization: secret-token");
                                }),
                        new CloudTasksStagingConfig());
        assertThatThrownBy(() -> failure.write("x", TestContexts.NO_OP))
                .hasMessageContaining("serialization failed")
                .hasMessageNotContaining("secret")
                .hasNoCause();
        Task valid = TestSinkConfigs.serializer().serialize("secret-body");
        for (Task invalid :
                new Task[] {
                    Task.getDefaultInstance(),
                    valid.toBuilder()
                            .setHttpRequest(
                                    valid.getHttpRequest().toBuilder()
                                            .setHttpMethod(HttpMethod.GET))
                            .build(),
                    Task.newBuilder()
                            .setAppEngineHttpRequest(
                                    AppEngineHttpRequest.newBuilder()
                                            .setHttpMethod(HttpMethod.POST)
                                            .setRelativeUri("secret-invalid-uri"))
                            .build(),
                    Task.newBuilder()
                            .setAppEngineHttpRequest(
                                    AppEngineHttpRequest.newBuilder()
                                            .setHttpMethod(HttpMethod.POST)
                                            .setRelativeUri("/")
                                            .setAppEngineRouting(
                                                    AppEngineRouting.newBuilder()
                                                            .setHost("secret-host")))
                            .build(),
                    Task.newBuilder()
                            .setAppEngineHttpRequest(
                                    AppEngineHttpRequest.newBuilder()
                                            .setHttpMethod(HttpMethod.POST)
                                            .setRelativeUri("/")
                                            .putHeaders("X-AppEngine-secret", "secret"))
                            .build()
                }) {
            var writer =
                    writer(
                            TestSinkConfigs.builder(TestSinkConfigs.QUEUE, element -> invalid),
                            new CloudTasksStagingConfig());
            assertThatThrownBy(() -> writer.write("x", TestContexts.NO_OP))
                    .hasMessageContaining("serialization failed")
                    .hasMessageNotContaining("secret")
                    .hasNoCause();
            assertThat(writer.getStagedTasks()).isZero();
        }
    }

    @Test
    void appEngineRequestIsPreservedAndFlushNeverDrainsOrSends() throws Exception {
        Task task =
                Task.newBuilder()
                        .setAppEngineHttpRequest(
                                AppEngineHttpRequest.newBuilder()
                                        .setRelativeUri("/task?q=1")
                                        .setHttpMethod(HttpMethod.PUT)
                                        .setBody(ByteString.copyFromUtf8("body"))
                                        .setAppEngineRouting(
                                                AppEngineRouting.newBuilder().setService("worker")))
                        .build();
        AtomicInteger serializations = new AtomicInteger();
        var writer =
                writer(
                        TestSinkConfigs.builder(
                                TestSinkConfigs.QUEUE,
                                element -> {
                                    serializations.incrementAndGet();
                                    return task;
                                }),
                        new CloudTasksStagingConfig());
        writer.write("x", TestContexts.NO_OP);
        writer.flush(false);
        writer.flush(true);
        assertThat(writer.getStagedTasks()).isEqualTo(1);
        var envelope = writer.prepareCommit().iterator().next();
        assertThat(envelope.parseTask().getAppEngineHttpRequest())
                .isEqualTo(task.getAppEngineHttpRequest());
        assertThat(serializations).hasValue(1);
        writer.close();
        writer.close();
        assertThatThrownBy(() -> writer.write("x", TestContexts.NO_OP))
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> writer.flush(true)).hasMessageContaining("closed");
    }

    @Test
    void rejectsDroppingHandlerAndForgedSettings() throws Exception {
        assertThatThrownBy(
                        () ->
                                writer(
                                        TestSinkConfigs.builder()
                                                .failedTaskHandler(FailureHandler.logAndDrop()),
                                        new CloudTasksStagingConfig()))
                .hasMessageContaining("failJob");
        var forged = new CloudTasksStagingConfig();
        var field = CloudTasksStagingConfig.class.getDeclaredField("maxStagedTasks");
        field.setAccessible(true);
        field.setInt(forged, 0);
        assertThatThrownBy(() -> writer(TestSinkConfigs.builder(), forged))
                .hasMessageContaining("maxStagedTasks");
        assertThat(new CloudTasksStagingConfig().getMaxStagedTasks()).isEqualTo(100_000);
    }

    @Test
    void resolverFailureStaysFatalWithoutExposingTheRecordOrCause() {
        var writer =
                writer(
                        CloudTasksSink.<String>builder()
                                .destinationResolver(
                                        (element, context) -> {
                                            throw new IllegalArgumentException(
                                                    "secret-credential:" + element);
                                        })
                                .serializer(TestSinkConfigs.serializer()),
                        new CloudTasksStagingConfig());
        assertThatThrownBy(() -> writer.write("secret-payload", TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("resolver failed")
                .hasMessageNotContaining("secret")
                .hasNoCause();
        assertThat(writer.getStagedTasks()).isZero();
        assertThat(random.calls).isZero();
    }

    @Test
    void invalidDispatchTokenAccountsNeverBecomeCheckpointedEnvelopes() {
        for (boolean oidc : new boolean[] {true, false}) {
            for (String account : new String[] {"", " \t\n"}) {
                HttpRequest.Builder request =
                        HttpRequest.newBuilder()
                                .setUrl("https://example.com/task")
                                .setHttpMethod(HttpMethod.POST);
                if (oidc) {
                    request.setOidcToken(OidcToken.newBuilder().setServiceAccountEmail(account));
                } else {
                    request.setOauthToken(OAuthToken.newBuilder().setServiceAccountEmail(account));
                }
                var writer =
                        writer(
                                TestSinkConfigs.builder(
                                        TestSinkConfigs.QUEUE,
                                        element ->
                                                Task.newBuilder().setHttpRequest(request).build()),
                                new CloudTasksStagingConfig());
                assertThatThrownBy(() -> writer.write("x", TestContexts.NO_OP))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("target constraints")
                        .hasNoCause();
                assertThat(writer.getStagedTasks()).isZero();
            }
        }
    }

    private CloudTasksStagedWriter<String> writer(
            CloudTasksSinkBuilder<String> builder, CloudTasksStagingConfig config) {
        return new CloudTasksStagedWriter<>(
                TestSinkConfigs.config(builder), config, metrics, time, random);
    }

    private static CloudTasksStagingConfig caps(int count, long bytes) {
        return new CloudTasksStagingConfig(
                count, bytes, Duration.ofHours(1), Duration.ofMinutes(5), Duration.ofSeconds(20));
    }

    private static final class CountingRandom extends SecureRandom {
        private static final long serialVersionUID = 1L;
        private int calls;

        @Override
        public void nextBytes(byte[] bytes) {
            assertThat(bytes).hasSize(16);
            bytes[bytes.length - 1] = (byte) ++calls;
        }
    }
}
