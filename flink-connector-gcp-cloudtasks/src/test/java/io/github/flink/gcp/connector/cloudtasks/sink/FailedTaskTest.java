/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.cloudtasks.sink;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FailedTask}, the Cloud Tasks half of the shared {@code FailedElement}. */
class FailedTaskTest {

    private static final QueueDestination QUEUE =
            QueueDestination.of("my-project", "asia-northeast1", "webhooks");

    private static final Task TASK =
            Task.newBuilder()
                    .setHttpRequest(
                            HttpRequest.newBuilder()
                                    .setUrl("https://api.example.com/v1/orders")
                                    .setHttpMethod(HttpMethod.POST)
                                    .setBody(ByteString.copyFromUtf8("order-1")))
                    .build();

    @Test
    void reportsTheSharedContract() {
        IOException cause = new IOException("rejected");

        FailedTask failed = FailedTask.of(QUEUE, TASK, "Cloud Tasks said no.", cause);

        assertThat(failed.getConnector()).isEqualTo("cloudtasks");
        assertThat(failed.describeDestination())
                .isEqualTo("projects/my-project/locations/asia-northeast1/queues/webhooks");
        assertThat(failed.getErrorMessage()).isEqualTo("Cloud Tasks said no.");
        assertThat(failed.getCause()).isSameAs(cause);
        assertThat(failed.getDestination()).isEqualTo(QUEUE);
        assertThat(failed.getTask()).isEqualTo(TASK);
    }

    @Test
    void carriesTheWholeTaskAsThePayload() throws Exception {
        FailedTask failed = FailedTask.of(QUEUE, TASK, "rejected", null);

        // Not just the body: a dead-letter consumer recovers the target, method and headers too.
        Task recovered = Task.parseFrom(failed.getPayloadBytes());
        assertThat(recovered).isEqualTo(TASK);
        assertThat(recovered.getHttpRequest().getUrl())
                .isEqualTo("https://api.example.com/v1/orders");
    }

    @Test
    void carriesNoPayloadWhenSerializationItselfFailed() {
        FailedTask failed = FailedTask.of(QUEUE, null, "The record could not be serialized.", null);

        assertThat(failed.getTask()).isNull();
        assertThat(failed.getPayloadBytes()).isNull();
    }

    @Test
    void rejectsAMissingDestinationOrMessage() {
        assertThatThrownBy(() -> FailedTask.of(null, TASK, "rejected", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("destination");
        assertThatThrownBy(() -> FailedTask.of(QUEUE, TASK, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("errorMessage");
    }

    @Test
    void describesItselfWithoutThePayload() {
        assertThat(FailedTask.of(QUEUE, TASK, "rejected", null).toString())
                .contains(TASK.getSerializedSize() + " bytes")
                .doesNotContain("order-1");
        assertThat(FailedTask.of(QUEUE, null, "rejected", null).toString()).contains("task=null");
    }
}
