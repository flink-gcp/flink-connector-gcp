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

package io.github.flink.gcp.connector.base.failure;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FailureHandler} and its built-in implementations. */
class FailureHandlerTest {

    /** A connector-neutral element, standing in for FailedRow and its future siblings. */
    private static final class TestElement implements FailedElement {

        @Override
        public String getConnector() {
            return "testconnector";
        }

        @Override
        public String describeDestination() {
            return "projects/p/things/t";
        }

        @Override
        public ByteString getPayloadBytes() {
            return ByteString.copyFromUtf8("payload");
        }

        @Override
        public String getErrorMessage() {
            return "bad element";
        }

        @Override
        public Throwable getCause() {
            return new RuntimeException("cause");
        }
    }

    private static final FailedElement ELEMENT = new TestElement();

    /** Recording queue; static sink list so a deserialized copy stays observable. */
    private static class RecordingDeadLetterQueue implements DeadLetterQueue {
        private static final long serialVersionUID = 1L;

        private static final List<FailedElement> offered = new ArrayList<>();
        private static final List<String> lifecycle = new ArrayList<>();
        private static FailureHandlerContext openedWith;

        private static void reset() {
            offered.clear();
            lifecycle.clear();
            openedWith = null;
        }

        @Override
        public void offer(FailedElement element) {
            offered.add(element);
        }

        @Override
        public void open(FailureHandlerContext context) {
            lifecycle.add("open");
            openedWith = context;
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

    @Test
    void failJobThrowsNamingConnectorDestinationAndCause() {
        assertThatThrownBy(() -> FailureHandler.failJob().handle(ELEMENT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("testconnector")
                .hasMessageContaining("projects/p/things/t")
                .hasMessageContaining("bad element")
                .hasRootCauseMessage("cause");
    }

    @Test
    void logAndDropReturnsNormally() {
        assertThatCode(() -> FailureHandler.logAndDrop().handle(ELEMENT))
                .doesNotThrowAnyException();
    }

    @Test
    void sendToDeadLetterQueueRoutesTheElement() throws Exception {
        RecordingDeadLetterQueue.reset();
        FailureHandler<FailedElement> handler =
                FailureHandler.sendToDeadLetterQueue(new RecordingDeadLetterQueue());

        handler.handle(ELEMENT);

        assertThat(RecordingDeadLetterQueue.offered).containsExactly(ELEMENT);
    }

    @Test
    void sendToDeadLetterQueueDrivesTheQueueLifecycle() throws Exception {
        RecordingDeadLetterQueue.reset();
        FailureHandler<FailedElement> handler =
                FailureHandler.sendToDeadLetterQueue(new RecordingDeadLetterQueue());
        FailureHandlerContext context =
                new DefaultFailureHandlerContext(3, new UnregisteredMetricsGroup());

        handler.open(context);
        handler.flush();
        handler.close();

        assertThat(RecordingDeadLetterQueue.lifecycle).containsExactly("open", "flush", "close");
        assertThat(RecordingDeadLetterQueue.openedWith.getSubtaskIndex()).isEqualTo(3);
        assertThat(RecordingDeadLetterQueue.openedWith.getMetricGroup())
                .isSameAs(context.getMetricGroup());
    }

    @Test
    void aDeadLetterQueueFailureFailsTheJob() {
        FailureHandler<FailedElement> handler =
                FailureHandler.sendToDeadLetterQueue(
                        element -> {
                            throw new IOException("queue unavailable");
                        });

        assertThatThrownBy(() -> handler.handle(ELEMENT))
                .isInstanceOf(IOException.class)
                .hasMessage("queue unavailable");
    }

    @Test
    void statelessBuiltInsHaveNoOpLifecycles() {
        FailureHandlerContext context =
                new DefaultFailureHandlerContext(0, new UnregisteredMetricsGroup());
        for (FailureHandler<FailedElement> handler :
                List.of(FailureHandler.failJob(), FailureHandler.logAndDrop())) {
            assertThatCode(
                            () -> {
                                handler.open(context);
                                handler.flush();
                                handler.close();
                            })
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void builtInHandlersSurviveSerializationRoundTrips() throws Exception {
        for (FailureHandler<FailedElement> handler :
                List.of(
                        FailureHandler.failJob(),
                        FailureHandler.logAndDrop(),
                        FailureHandler.sendToDeadLetterQueue(new RecordingDeadLetterQueue()))) {
            FailureHandler<FailedElement> copy =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(handler),
                            getClass().getClassLoader());
            assertThat(copy).isExactlyInstanceOf(handler.getClass());
        }
    }

    @Test
    void deserializedDeadLetterHandlerStillRoutes() throws Exception {
        RecordingDeadLetterQueue.reset();
        FailureHandler<FailedElement> copy =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(
                                FailureHandler.sendToDeadLetterQueue(
                                        new RecordingDeadLetterQueue())),
                        getClass().getClassLoader());

        copy.handle(ELEMENT);

        assertThat(RecordingDeadLetterQueue.offered).containsExactly(ELEMENT);
    }
}
