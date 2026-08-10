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

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadClientSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.ReadClientRowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStream;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins the ways the BigQuery emulator's read path differs from BigQuery's own.
 *
 * <p>Each deviation is asserted as it behaves <em>today</em>, so the day an image bump fixes one
 * the build says so rather than leaving a workaround nobody dares remove. Beside each is a {@link
 * Disabled} test carrying the behaviour BigQuery actually has (measured 2026-08-09 against the real
 * service), to be enabled when the deviation retires.
 *
 * <p>Nothing here asserts what the connector does — {@code BigQuerySourceEmulatorITCase} does that.
 * These are measurements of the emulator, kept where a reader of the source's tests will find them.
 */
class BigQueryEmulatorReadDeviationITCase extends AbstractBigQuerySourceEmulatorITCase {

    private static final String TABLE = "deviations";
    private static final int ROWS = 5;

    @BeforeAll
    static void seed() throws Exception {
        createTable(
                TABLE,
                Field.newBuilder("id", StandardSQLTypeName.INT64)
                        .setMode(Field.Mode.REQUIRED)
                        .build());
        insert(TABLE, "id", "(0), (1), (2), (3), (4)");
    }

    @Test
    void rejectsMoreThanOneStream() throws Exception {
        // BigQuery caps at the requested count instead, and returns fewer when the table is small.
        try (ReadSessionCreator creator = sessionCreator()) {
            assertThatThrownBy(() -> creator.create(request(2)))
                    .hasMessageContaining("currently supported only one stream");
        }
    }

    @Test
    @Disabled(
            "Enable when the emulator honours maxStreamCount; see the deviation table in the docs")
    void capsTheStreamCountAsBigQueryDoes() throws Exception {
        try (ReadSessionCreator creator = sessionCreator()) {
            assertThat(creator.create(request(2)).getStreamsCount()).isLessThanOrEqualTo(2);
        }
    }

    @Test
    void ignoresTheRequestedOffsetAndAnswersFromTheStart() throws Exception {
        String stream = createStream();

        try (RowStreamOpener opener = streamOpener()) {
            assertThat(rowCount(opener, stream, 3)).isEqualTo(ROWS);
            assertThat(rowCount(opener, stream, ROWS)).isEqualTo(ROWS);
            assertThat(rowCount(opener, stream, 100)).isEqualTo(ROWS);
        }
    }

    @Test
    @Disabled(
            "Enable when the emulator honours ReadRowsRequest.offset; the resume tests can then"
                    + " stop being emulator-free")
    void resumesAtTheRequestedOffsetAsBigQueryDoes() throws Exception {
        String stream = createStream();

        try (RowStreamOpener opener = streamOpener()) {
            assertThat(rowCount(opener, stream, 3)).isEqualTo(ROWS - 3);
            assertThat(rowCount(opener, stream, ROWS)).isZero();
        }
    }

    @Test
    void answersAWholeTableInOneResponseBlock() throws Exception {
        // BigQuery splits a stream into blocks of up to about 128 MiB. The emulator's single block
        // is what makes its ITCases the strictest test of the per-fetch cap: the reader has to
        // resume inside a block rather than between responses.
        String stream = createStream();

        try (RowStreamOpener opener = streamOpener()) {
            assertThat(blocks(opener, stream)).isEqualTo(1);
        }
    }

    @Test
    void namesTheAvroSchemaAfterTheProjectAndDataset() throws Exception {
        // BigQuery answers with a record named __root__ and no namespace. The emulator's namespace
        // is why this module's emulator harness uses a project id without a hyphen: a hyphen is
        // illegal in an Avro namespace, and the schema then fails to parse before a row is decoded.
        try (ReadSessionCreator creator = sessionCreator()) {
            assertThat(creator.create(request(0)).getAvroSchema().getSchema())
                    .contains("\"namespace\":\"" + PROJECT + "." + DATASET + "\"")
                    .doesNotContain("__root__");
        }
    }

    @Test
    void expiresTheSessionSoonerThanBigQueryDoes() throws Exception {
        // BigQuery gives six hours; the emulator gives one. Nothing depends on the value yet, and
        // this is what says so if that changes.
        try (ReadSessionCreator creator = sessionCreator()) {
            ReadSession session = creator.create(request(0));
            long lifetimeSeconds =
                    session.getExpireTime().getSeconds() - System.currentTimeMillis() / 1000;
            // Bounded from below as well: an emulator that stopped setting expireTime at all
            // would answer with a large negative number and pass a one-sided assertion.
            assertThat(lifetimeSeconds).isBetween(1L, 2L * 3600);
        }
    }

    private static CreateReadSessionRequest request(int maxStreamCount) {
        CreateReadSessionRequest.Builder request =
                CreateReadSessionRequest.newBuilder()
                        .setParent("projects/" + PROJECT)
                        .setReadSession(
                                ReadSession.newBuilder()
                                        .setTable(destination(TABLE).toTablePath())
                                        .setDataFormat(DataFormat.AVRO)
                                        .build());
        if (maxStreamCount > 0) {
            request.setMaxStreamCount(maxStreamCount);
        }
        return request.build();
    }

    private static String createStream() throws Exception {
        try (ReadSessionCreator creator = sessionCreator()) {
            return creator.create(request(0)).getStreams(0).getName();
        }
    }

    private static ReadSessionCreator sessionCreator() {
        return new ReadClientSessionCreator(EmulatorEndpoint.parse(grpcEndpoint()));
    }

    private static RowStreamOpener streamOpener() {
        return new ReadClientRowStreamOpener(
                EmulatorEndpoint.parse(grpcEndpoint()),
                BigQuerySourceBuilder.DEFAULT_RETRY_MAX_ATTEMPTS);
    }

    private static long rowCount(RowStreamOpener opener, String stream, long offset)
            throws Exception {
        List<Long> counts = new ArrayList<>();
        try (RowStream rows = opener.open(stream, offset)) {
            ReadRowsResponse response;
            while ((response = rows.next()) != null) {
                counts.add(response.getRowCount());
            }
        }
        return counts.stream().mapToLong(Long::longValue).sum();
    }

    private static int blocks(RowStreamOpener opener, String stream) throws Exception {
        int blocks = 0;
        try (RowStream rows = opener.open(stream, 0)) {
            while (rows.next() != null) {
                blocks++;
            }
        }
        return blocks;
    }
}
