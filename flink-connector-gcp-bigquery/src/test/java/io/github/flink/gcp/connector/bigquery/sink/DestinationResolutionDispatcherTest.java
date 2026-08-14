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

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.testutils.TestContexts;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the internal destination-resolution double dispatch. */
class DestinationResolutionDispatcherTest {

    private static final SinkWriter.Context CONTEXT = TestContexts.NO_OP;

    @Test
    void dispatchesTableDestinationWithOriginalInputs() throws Exception {
        TableDestination destination = TableDestination.of("project", "dataset", "table");
        String element = new String("record");
        RecordingVisitor visitor = new RecordingVisitor();

        DestinationResolutionDispatcher.dispatch(destination, element, CONTEXT, visitor);

        assertThat(visitor.destination).isSameAs(destination);
        assertThat(visitor.failure).isNull();
        assertThat(visitor.element).isSameAs(element);
        assertThat(visitor.context).isSameAs(CONTEXT);
    }

    @Test
    void dispatchesUnroutableRecordWithOriginalInputs() throws Exception {
        UnroutableRecord failure =
                UnroutableRecord.of(ByteString.copyFromUtf8("record"), "unknown tenant");
        String element = new String("record");
        RecordingVisitor visitor = new RecordingVisitor();

        DestinationResolutionDispatcher.dispatch(failure, element, CONTEXT, visitor);

        assertThat(visitor.failure).isSameAs(failure);
        assertThat(visitor.destination).isNull();
        assertThat(visitor.element).isSameAs(element);
        assertThat(visitor.context).isSameAs(CONTEXT);
    }

    @Test
    void rejectsNullResolutionWithStableDiagnostic() {
        assertThatThrownBy(
                        () ->
                                DestinationResolutionDispatcher.dispatch(
                                        null, "record", CONTEXT, new RecordingVisitor()))
                .isInstanceOf(IOException.class)
                .hasMessage("The destination resolver returned null for a record.");
    }

    private static final class RecordingVisitor
            implements DestinationResolutionDispatcher.Visitor<String> {

        private TableDestination destination;
        private UnroutableRecord failure;
        private String element;
        private SinkWriter.Context context;

        @Override
        public void visit(
                TableDestination destination, String element, SinkWriter.Context context) {
            this.destination = destination;
            this.element = element;
            this.context = context;
        }

        @Override
        public void visit(UnroutableRecord failure, String element, SinkWriter.Context context) {
            this.failure = failure;
            this.element = element;
            this.context = context;
        }
    }
}
