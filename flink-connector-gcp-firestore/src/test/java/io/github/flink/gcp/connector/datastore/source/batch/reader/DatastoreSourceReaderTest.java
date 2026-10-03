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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import io.github.flink.gcp.connector.datastore.source.TestSources;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** The page reader's owner: the subtask's source reader closes it, once. */
class DatastoreSourceReaderTest {

    @Test
    void closingTheReaderClosesTheSharedPageReaderOnce() throws Exception {
        ScriptedQueryPageReader pages = new ScriptedQueryPageReader(List.of("a"));
        SourceReader<String, QuerySplit> reader =
                TestSources.source(builder -> TestSources.withPageReader(builder.kind("K"), pages))
                        .createReader(
                                new FakeSourceReaderContext(
                                        InternalSourceReaderMetricGroup.mock(
                                                new MetricListener().getMetricGroup())));

        reader.close();

        assertThat(pages.closeCalls()).isOne();
    }
}
