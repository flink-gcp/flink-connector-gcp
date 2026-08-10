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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import io.github.flink.gcp.connector.bigquery.source.reader.RowStream;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BigQueryStorageReadSource}. */
class BigQueryStorageReadSourceTest {

    @Test
    void wiresTheClientsRetriesToTheReadersCounter() throws Exception {
        // The client library retries a broken ReadRows itself, and a stream that keeps failing and
        // resuming makes progress — so it never reaches retryMaxAttempts, never fails the job, and
        // reports nothing at all except through this counter.
        MetricListener listener = new MetricListener();
        SourceReaderMetricGroup metricGroup =
                InternalSourceReaderMetricGroup.mock(listener.getMetricGroup());
        RecordingOpener opener = new RecordingOpener();

        try (SourceReader<GenericRecord, BigQueryReadStreamSplit> reader =
                new BigQueryStorageReadSource<>(
                                TestSources.config(builder -> builder.rowStreamOpener(opener)))
                        .createReader(new FakeSourceReaderContext(metricGroup))) {
            assertThat(opener.onRetry).isNotNull();
            opener.onRetry.run();
            opener.onRetry.run();
        }

        assertThat(listener.getCounter("readRetries")).isPresent();
        assertThat(listener.getCounter("readRetries").get().getCount()).isEqualTo(2);
    }

    /** An opener that keeps whatever retry listener it was handed and opens nothing. */
    private static final class RecordingOpener implements RowStreamOpener {

        private static final long serialVersionUID = 1L;

        @Nullable private transient Runnable onRetry;

        @Override
        public void setRetryListener(Runnable onRetry) {
            this.onRetry = onRetry;
        }

        @Override
        public RowStream open(String streamName, long offset) throws IOException {
            throw new IOException("This opener opens nothing.");
        }

        @Override
        public void close() {}
    }
}
