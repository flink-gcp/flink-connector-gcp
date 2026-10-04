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

package io.github.flink.gcp.connector.base.lineage.internal;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Flink 2.x declarations must survive metadata wrapping, including Java serialization. */
class TableLineageWatermarksTest {
    @Test
    void nonemptyDeclarationsReachFlinkUnchanged() {
        Set<WatermarkDeclaration> declarations = declarations();
        Source<String, SourceSplit, Void> wrapped = wrap(new DeclaringSource(declarations));

        assertThat(wrapped.declareWatermarks()).isSameAs(declarations);
    }

    @Test
    void declarationsRemainDelegatedAfterSerialization() throws Exception {
        TableLineageSource<String, SourceSplit, Void> wrapped =
                wrap(new DeclaringSource(declarations()));
        TableLineageSource<String, SourceSplit, Void> restored =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(wrapped), getClass().getClassLoader());

        assertThat(restored.declareWatermarks()).isSameAs(restored.delegate().declareWatermarks());
        assertThat(restored.declareWatermarks())
                .extracting(WatermarkDeclaration::getIdentifier)
                .containsExactlyInAnyOrder("progress", "lag");
    }

    private static Set<WatermarkDeclaration> declarations() {
        return Set.of(
                WatermarkDeclarations.newBuilder("progress").typeLong().build(),
                WatermarkDeclarations.newBuilder("lag").typeLong().build());
    }

    private static TableLineageSource<String, SourceSplit, Void> wrap(DeclaringSource source) {
        return TableLineageSource.of(source, Types.STRING, "c.db.t", "spanner", List.of());
    }

    private static final class DeclaringSource implements Source<String, SourceSplit, Void> {
        private static final long serialVersionUID = 1L;
        private final Set<WatermarkDeclaration> declarations;

        private DeclaringSource(Set<WatermarkDeclaration> declarations) {
            this.declarations = declarations;
        }

        @Override
        public Set<? extends WatermarkDeclaration> declareWatermarks() {
            return declarations;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<String, SourceSplit> createReader(SourceReaderContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitEnumerator<SourceSplit, Void> createEnumerator(
                SplitEnumeratorContext<SourceSplit> context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SplitEnumerator<SourceSplit, Void> restoreEnumerator(
                SplitEnumeratorContext<SourceSplit> context, Void checkpoint) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleVersionedSerializer<SourceSplit> getSplitSerializer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
            throw new UnsupportedOperationException();
        }
    }
}
