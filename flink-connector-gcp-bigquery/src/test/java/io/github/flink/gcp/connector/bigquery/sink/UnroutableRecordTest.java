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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.DeadLetterQueue;
import io.github.flink.gcp.connector.base.failure.DefaultFailureHandlerContext;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.bigquery.sink.failure.BigQueryFailure;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the explicit destination-resolution failure result. */
class UnroutableRecordTest {

    @Test
    void exposesTheFailureHandlerContract() {
        ByteString payload = ByteString.copyFromUtf8("original-record");

        UnroutableRecord failure = UnroutableRecord.of(payload, "unknown tenant");

        assertThat(failure).isInstanceOf(DestinationResolution.class);
        assertThat(failure).isInstanceOf(BigQueryFailure.class);
        assertThat(failure.getConnector()).isEqualTo("bigquery");
        assertThat(failure.describeDestination()).isEqualTo("unresolved");
        assertThat(failure.getPayloadBytes()).isSameAs(payload);
        assertThat(failure.getErrorMessage()).isEqualTo("unknown tenant");
        assertThat(failure.getCause()).isNull();
        assertThat(failure).isNotInstanceOf(Serializable.class);
    }

    @Test
    void requiresPayloadAndReason() {
        assertThatThrownBy(() -> UnroutableRecord.of(null, "unknown tenant"))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("payloadBytes");
        assertThatThrownBy(() -> UnroutableRecord.of(ByteString.EMPTY, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("errorMessage");
    }

    @Test
    void deadLetterPolicyPreservesRoutingDiagnosticsAndLifecycle() throws Exception {
        RecordingDeadLetterQueue queue = new RecordingDeadLetterQueue();
        FailureHandler<BigQueryFailure> handler = FailureHandler.sendToDeadLetterQueue(queue);
        UnroutableRecord failure =
                UnroutableRecord.of(ByteString.copyFromUtf8("original"), "unknown tenant");

        handler.open(new DefaultFailureHandlerContext(2, new UnregisteredMetricsGroup()));
        handler.handle(failure);
        handler.flush();
        handler.close();

        assertThat(queue.elements).containsExactly(failure);
        assertThat(queue.elements.get(0).describeDestination()).isEqualTo("unresolved");
        assertThat(queue.elements.get(0).getPayloadBytes().toStringUtf8()).isEqualTo("original");
        assertThat(queue.elements.get(0).getErrorMessage()).isEqualTo("unknown tenant");
        assertThat(queue.lifecycle).containsExactly("open", "flush", "close");
    }

    private static final class RecordingDeadLetterQueue implements DeadLetterQueue {
        private static final long serialVersionUID = 1L;

        private final List<FailedElement> elements = new ArrayList<>();
        private final List<String> lifecycle = new ArrayList<>();

        @Override
        public void open(io.github.flink.gcp.connector.base.failure.FailureHandlerContext context) {
            lifecycle.add("open");
        }

        @Override
        public void offer(FailedElement element) {
            elements.add(element);
        }

        @Override
        public void flush() {
            lifecycle.add("flush");
        }

        @Override
        public void close() {
            lifecycle.add("close");
        }
    }
}
