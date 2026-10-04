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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Holds the table source adapter to its contract: every runtime operation reaches the wrapped
 * source unchanged, so a restore restores rather than plans again.
 */
class FirestoreTableLineageTest {

    @Test
    void theTableSourceDelegatesEveryOperation() throws Exception {
        RecordingSource delegate = new RecordingSource();
        Source<String, SourceSplit, String> wrapped =
                FirestoreTableLineage.of("t", DatabaseDestination.of("p"), "c")
                        .source(delegate, Types.STRING);

        assertThat(wrapped.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
        wrapped.createReader(null);
        wrapped.createEnumerator(null);
        wrapped.restoreEnumerator(null, "state");
        assertThat(wrapped.getSplitSerializer()).isSameAs(RecordingSource.SPLITS);
        assertThat(wrapped.getEnumeratorCheckpointSerializer()).isSameAs(RecordingSource.STATE);
        assertThat(
                        ((org.apache.flink.api.java.typeutils.ResultTypeQueryable<?>) wrapped)
                                .getProducedType())
                .isEqualTo(Types.STRING);
        assertThat(delegate.calls)
                .containsExactly(
                        "boundedness", "createReader", "createEnumerator", "restore:state");
    }

    private static final class RecordingSource implements Source<String, SourceSplit, String> {
        static final SimpleVersionedSerializer<SourceSplit> SPLITS = serializer();
        static final SimpleVersionedSerializer<String> STATE = serializer();
        final List<String> calls = new ArrayList<>();

        @Override
        public Boundedness getBoundedness() {
            calls.add("boundedness");
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<String, SourceSplit> createReader(SourceReaderContext context) {
            calls.add("createReader");
            return null;
        }

        @Override
        public SplitEnumerator<SourceSplit, String> createEnumerator(
                SplitEnumeratorContext<SourceSplit> context) {
            calls.add("createEnumerator");
            return null;
        }

        @Override
        public SplitEnumerator<SourceSplit, String> restoreEnumerator(
                SplitEnumeratorContext<SourceSplit> context, String checkpoint) {
            calls.add("restore:" + checkpoint);
            return null;
        }

        @Override
        public SimpleVersionedSerializer<SourceSplit> getSplitSerializer() {
            return SPLITS;
        }

        @Override
        public SimpleVersionedSerializer<String> getEnumeratorCheckpointSerializer() {
            return STATE;
        }

        private static <T> SimpleVersionedSerializer<T> serializer() {
            return new SimpleVersionedSerializer<T>() {
                @Override
                public int getVersion() {
                    return 1;
                }

                @Override
                public byte[] serialize(T obj) {
                    return new byte[0];
                }

                @Override
                public T deserialize(int version, byte[] serialized) {
                    return null;
                }
            };
        }
    }
}
