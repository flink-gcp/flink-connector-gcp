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

import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FailedRowHandler} and its built-in implementations. */
class FailedRowHandlerTest {

    private static final FailedRow ROW =
            FailedRow.of(
                    TableDestination.of("p", "d", "t"),
                    ByteString.copyFromUtf8("row"),
                    "bad row",
                    new RuntimeException("cause"));

    /** Recording queue; static sink list so a deserialized copy stays observable. */
    private static class RecordingDeadLetterQueue implements DeadLetterQueue {
        private static final long serialVersionUID = 1L;

        private static final List<FailedRow> offered = new ArrayList<>();

        @Override
        public void offer(FailedRow row) {
            offered.add(row);
        }
    }

    @Test
    void failJobThrowsWithRowDetailAndCause() {
        assertThatThrownBy(() -> FailedRowHandler.failJob().handle(ROW))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("p.d.t")
                .hasMessageContaining("bad row")
                .hasRootCauseMessage("cause");
    }

    @Test
    void logAndDropReturnsNormally() {
        assertThatCode(() -> FailedRowHandler.logAndDrop().handle(ROW)).doesNotThrowAnyException();
    }

    @Test
    void sendToDeadLetterQueueRoutesTheRow() throws Exception {
        RecordingDeadLetterQueue.offered.clear();
        FailedRowHandler handler =
                FailedRowHandler.sendToDeadLetterQueue(new RecordingDeadLetterQueue());

        handler.handle(ROW);

        assertThat(RecordingDeadLetterQueue.offered).containsExactly(ROW);
    }

    @Test
    void builtInHandlersSurviveSerializationRoundTrips() throws Exception {
        for (FailedRowHandler handler :
                List.of(
                        FailedRowHandler.failJob(),
                        FailedRowHandler.logAndDrop(),
                        FailedRowHandler.sendToDeadLetterQueue(new RecordingDeadLetterQueue()))) {
            FailedRowHandler copy =
                    InstantiationUtil.deserializeObject(
                            InstantiationUtil.serializeObject(handler),
                            getClass().getClassLoader());
            assertThat(copy).isExactlyInstanceOf(handler.getClass());
        }
    }

    @Test
    void deserializedDeadLetterHandlerStillRoutes() throws Exception {
        RecordingDeadLetterQueue.offered.clear();
        FailedRowHandler copy =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(
                                FailedRowHandler.sendToDeadLetterQueue(
                                        new RecordingDeadLetterQueue())),
                        getClass().getClassLoader());

        copy.handle(ROW);

        assertThat(RecordingDeadLetterQueue.offered).containsExactly(ROW);
    }
}
