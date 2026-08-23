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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.github.flink.gcp.connector.bigtable.source.TestSources;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplit;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRangeSplitState;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigtableSourceReader}. */
@Timeout(30)
class BigtableSourceReaderTest {

    private final TestReaderMetrics metrics = new TestReaderMetrics();
    private final FakeSourceReaderContext context =
            new FakeSourceReaderContext(metrics.metricGroup());

    @AfterEach
    void forgetScriptedTables() {
        ScriptedRowStreamOpener.reset();
    }

    private BigtableSourceReader<String> reader(ScriptedRowStreamOpener opener) {
        Supplier<SplitReader<Row, RowRangeSplit>> splitReaders =
                () ->
                        new BigtableSplitReader(
                                TestSources.TABLE, opener, null, 10, metrics.metrics());
        return new BigtableSourceReader<>(
                splitReaders,
                new BigtableRecordEmitter<>(
                        new TestSources.RowKeyDeserializer(), metrics.metrics()),
                new Configuration(),
                context,
                opener);
    }

    @Test
    void asksForASplitWhenItStartsWithNone() throws Exception {
        BigtableSourceReader<String> reader =
                reader(ScriptedRowStreamOpener.over("reader-start", "a"));

        reader.start();

        assertThat(context.splitRequests()).isEqualTo(1);
        reader.close();
    }

    @Test
    void asksForNothingWhenItStartsWithARestoredSplit() throws Exception {
        // A restored reader is given its splits before it is started, so asking again would take a
        // second split it has no capacity to read.
        BigtableSourceReader<String> reader =
                reader(ScriptedRowStreamOpener.over("reader-restored", "a"));
        reader.addSplits(
                Collections.singletonList(new RowRangeSplit("0", ByteStringRange.unbounded())));

        reader.start();

        assertThat(context.splitRequests()).isZero();
        reader.close();
    }

    @Test
    void closesTheOpenerItOwns() throws Exception {
        // The reader owns the opener the split readers share, and nothing else releases it: without
        // this every subtask would leak a Bigtable client, invisibly, for the life of the task.
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("reader-close", "a");
        BigtableSourceReader<String> reader = reader(opener);
        reader.start();

        reader.close();

        assertThat(opener.closeCalls()).isEqualTo(1);
    }

    @Test
    void closesTheOpenerAfterItsFetcherStream() throws Exception {
        ScriptedRowStreamOpener opener = ScriptedRowStreamOpener.over("reader-order", "a");
        opener.blockAfter(0, ScriptedRowStreamOpener.CancelBehaviour.ENDS_QUIETLY);
        BigtableSourceReader<String> reader = reader(opener);
        reader.addSplits(
                Collections.singletonList(new RowRangeSplit("0", ByteStringRange.unbounded())));
        reader.start();
        opener.awaitBlocked();

        reader.close();

        assertThat(opener.lifecycleEvents()).containsExactly("stream", "opener");
    }

    @Test
    void checkpointsASplitAtTheRangeItHasLeft() {
        // toSplitType is what a checkpoint stores, and it is reached only through the reader.
        BigtableSourceReader<String> reader =
                reader(ScriptedRowStreamOpener.over("reader-checkpoint", "a"));
        RowRangeSplitState state =
                reader.initializedState(
                        new RowRangeSplit("0", ByteStringRange.unbounded().endOpen("z")));
        state.recordEmitted(com.google.protobuf.ByteString.copyFromUtf8("m"));

        RowRangeSplit checkpointed = reader.toSplitType("0", state);

        assertThat(checkpointed.splitId()).isEqualTo("0");
        assertThat(checkpointed.getRange())
                .isEqualTo(ByteStringRange.unbounded().startOpen("m").endOpen("z"));
    }
}
