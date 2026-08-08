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

package io.github.flink.gcp.connector.bigquery.sink.storage;

import io.github.flink.gcp.connector.bigquery.sink.BigQuerySink;
import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.WriteMethod;
import io.github.flink.gcp.connector.bigquery.sink.storage.committer.BufferedStreamCommitter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Does the sink hand the committer the disposition the job was configured with?
 *
 * <p>A seam worth its own test because it is invisible from both sides: {@code
 * BufferedStreamCommitterTest} passes the disposition itself, and every emulator and unit test
 * would stay green if {@code createCommitter} hardcoded one — the only thing that would notice is a
 * real-GCP job losing a race it usually wins.
 */
class BigQueryBufferedStreamSinkCommitterTest {

    private static final TableDestination DESTINATION = TableDestination.of("p", "d", "t");

    @ParameterizedTest
    @EnumSource(CreateDisposition.class)
    void theSinkGivesTheCommitterItsOwnCreateDisposition(CreateDisposition disposition) {
        BufferedStreamCommitter committer = committerOf(sink(disposition));

        assertThat(committer.getCreateDisposition()).isEqualTo(disposition);
    }

    private static BigQueryBufferedStreamSink<String> sink(CreateDisposition disposition) {
        return (BigQueryBufferedStreamSink<String>)
                BigQuerySink.<String>builder()
                        .writeMethod(WriteMethod.STORAGE_API_EXACTLY_ONCE)
                        .destination(DESTINATION)
                        .serializer(new NameValueRowSerializer())
                        .createDisposition(disposition)
                        .bufferedStreamOptions(BufferedStreamOptions.builder().build())
                        .build();
    }

    private static BufferedStreamCommitter committerOf(BigQueryBufferedStreamSink<String> sink) {
        // A null context, deliberately: createCommitter does not read one, and the committer's
        // BigQuery client is built lazily on the first flush, so nothing here reaches the network.
        // Should createCommitter ever start reading its context, this fails loudly, which is the
        // right outcome rather than a reason to build a stub now.
        return (BufferedStreamCommitter) sink.createCommitter(null);
    }
}
