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

package io.github.flink.gcp.connector.bigquery.source.reader;

import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import io.github.flink.gcp.connector.bigquery.source.TestRows;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroRowCursorTest {

    @Test
    void decodesEveryRowOfABlockInOrder() throws Exception {
        AvroRowCursor cursor = new AvroRowCursor(TestRows.SCHEMA, null);
        cursor.reset(TestRows.block(TestRows.rows(3)));

        assertThat(cursor.next().get("name")).hasToString("row-0");
        assertThat(cursor.next().get("name")).hasToString("row-1");
        assertThat(cursor.next().get("name")).hasToString("row-2");
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void resumesWhereItStoppedInsideABlock() throws Exception {
        AvroRowCursor cursor = new AvroRowCursor(TestRows.SCHEMA, null);
        cursor.reset(TestRows.block(TestRows.rows(4)));

        cursor.next();
        cursor.next();

        // Two rows taken, two left: this is what lets a per-fetch cap split one block into several
        // batches with a checkpoint able to land between them.
        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next().get("name")).hasToString("row-2");
        assertThat(cursor.next().get("name")).hasToString("row-3");
        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void reusesOneDecoderAcrossBlocks() throws Exception {
        AvroRowCursor cursor = new AvroRowCursor(TestRows.SCHEMA, null);
        List<ReadRowsResponse> blocks = TestRows.blocks(TestRows.rows(4), 2);

        cursor.reset(blocks.get(0));
        cursor.next();
        BinaryDecoder first = cursor.decoder();
        cursor.reset(blocks.get(1));

        assertThat(cursor.decoder()).isSameAs(first);
        assertThat(cursor.next().get("name")).hasToString("row-2");
    }

    @Test
    void resolvesTheReaderSchemaAgainstTheWriterSchema() throws Exception {
        // A user's hand-written reader schema takes a subset of the columns; Avro resolution drops
        // the rest. The record the cursor produces then carries the reader's schema, which is what
        // the produced TypeInformation was derived from.
        Schema readerSchema =
                new Schema.Parser()
                        .parse(
                                "{\"type\":\"record\",\"name\":\"Row\",\"fields\":["
                                        + "{\"name\":\"name\",\"type\":\"string\"}]}");
        AvroRowCursor cursor = new AvroRowCursor(TestRows.SCHEMA, readerSchema);
        cursor.reset(TestRows.block(TestRows.rows(1)));

        assertThat(cursor.next().getSchema()).isEqualTo(readerSchema);
    }

    @Test
    void discardsTheRestOfABlock() throws Exception {
        AvroRowCursor cursor = new AvroRowCursor(TestRows.SCHEMA, null);
        cursor.reset(TestRows.block(TestRows.rows(3)));
        cursor.next();

        cursor.discard();

        assertThat(cursor.hasNext()).isFalse();
    }

    @Test
    void handlesABlockWithNoRows() {
        AvroRowCursor cursor = new AvroRowCursor(TestRows.SCHEMA, null);
        cursor.reset(TestRows.block(Collections.emptyList()));

        assertThat(cursor.hasNext()).isFalse();
        assertThatThrownBy(cursor::next).isInstanceOf(IllegalStateException.class);
    }
}
