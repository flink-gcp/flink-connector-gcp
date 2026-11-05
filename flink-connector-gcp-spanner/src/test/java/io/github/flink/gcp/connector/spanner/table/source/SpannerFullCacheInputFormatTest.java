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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.TestSources;
import io.github.flink.gcp.connector.spanner.source.TestStructs;
import io.github.flink.gcp.connector.spanner.source.batch.SpannerBatchReadSource;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.ScriptedPartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.reader.ScriptedStructStreamOpener;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The full lookup cache's loader, over scripted partition planning and partition reads. */
class SpannerFullCacheInputFormatTest {

    private static final DataType PHYSICAL =
            DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT().notNull()));
    private static final SpannerTableSchemaConverter SCHEMA =
            SpannerTableSchemaConverter.of(
                    (RowType) PHYSICAL.getLogicalType(),
                    new int[] {0},
                    Dialect.GOOGLE_STANDARD_SQL,
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    Collections.emptyMap());

    @AfterEach
    void forgetRecordings() {
        ScriptedPartitionPlanner.reset();
        ScriptedStructStreamOpener.reset();
    }

    @Test
    void oneLoadReadsEveryPartitionOfOneSnapshotAndReleasesIt() throws Exception {
        ScriptedPartitionPlanner.Factory planning =
                ScriptedPartitionPlanner.planning("load", "p0", "p1");
        ScriptedStructStreamOpener opener = opener("load");
        SpannerFullCacheInputFormat initial =
                format(
                        planning,
                        opener,
                        TimestampBound.ofExactStaleness(15, TimeUnit.SECONDS),
                        true);

        assertThat(initial.createInputSplits(1)).hasSize(1);
        assertThat(load(initial)).containsExactly(1L, 2L, 3L);

        assertThat(planning.plans()).isEqualTo(1);
        assertThat(planning.bounds())
                .containsExactly(TimestampBound.ofExactStaleness(15, TimeUnit.SECONDS));
        assertThat(planning.dataBoostFlags()).containsExactly(true);
        assertThat(opener.openedTokens()).containsExactly("p0", "p1");
        assertThat(opener.streamCloses()).isEqualTo(2);
        assertThat(opener.closes()).isEqualTo(1);
        assertThat(planning.closes()).isEqualTo(1);
    }

    @Test
    void everyLoadPlansAFreshBatchRead() throws Exception {
        ScriptedPartitionPlanner.Factory planning =
                ScriptedPartitionPlanner.planning("reload", "p0", "p1");
        ScriptedStructStreamOpener opener = opener("reload");
        SpannerFullCacheInputFormat initial =
                format(planning, opener, TimestampBound.strong(), false);

        assertThat(load(initial)).containsExactly(1L, 2L, 3L);
        assertThat(load(initial)).containsExactly(1L, 2L, 3L);

        assertThat(planning.plans()).isEqualTo(2);
        assertThat(planning.minted()).hasSize(2).allMatch(ScriptedPartitionPlanner::isClosed);
        assertThat(opener.closes()).isEqualTo(2);
    }

    @Test
    void aFailedPartitionFailsTheLoadAndCloseStillReleasesTheRead() throws Exception {
        ScriptedPartitionPlanner.Factory planning =
                ScriptedPartitionPlanner.planning("fails", "p0", "p1");
        ScriptedStructStreamOpener opener = opener("fails");
        opener.failReadAfter(1, unavailable());
        SpannerFullCacheInputFormat format =
                copy(format(planning, opener, TimestampBound.strong(), false));
        format.open(new GenericInputSplit(0, 1));

        assertThatThrownBy(() -> drain(format)).isInstanceOf(SpannerException.class);
        // Flink's loader closes the format in a finally block after any failure.
        format.close();

        assertThat(opener.openedTokens()).containsExactly("p0");
        assertThat(opener.streamCloses()).isEqualTo(1);
        assertThat(opener.closes()).isEqualTo(1);
        assertThat(planning.closes()).isEqualTo(1);
    }

    @Test
    void aFailedPlanReleasesThePlannerBeforeOpenThrows() throws Exception {
        ScriptedPartitionPlanner.Factory planning = ScriptedPartitionPlanner.planning("plan", "p0");
        planning.failNextPlan(unavailable());
        ScriptedStructStreamOpener opener = opener("plan");
        SpannerFullCacheInputFormat format =
                copy(format(planning, opener, TimestampBound.strong(), false));

        assertThatThrownBy(() -> format.open(new GenericInputSplit(0, 1)))
                .isInstanceOf(SpannerException.class);
        // Flink's loader closes the format after a failed open too; nothing is released twice.
        format.close();

        assertThat(planning.closes()).isEqualTo(1);
        assertThat(opener.closes()).isEqualTo(1);
        assertThat(opener.openedTokens()).isEmpty();
    }

    @Test
    void aLoadOverNoPartitionsIsEmptyAndStillReleasesTheRead() throws Exception {
        ScriptedPartitionPlanner.Factory planning = ScriptedPartitionPlanner.planning("empty");
        ScriptedStructStreamOpener opener = opener("empty");
        SpannerFullCacheInputFormat initial =
                format(planning, opener, TimestampBound.strong(), false);

        assertThat(initial.createInputSplits(4)).hasSize(1);
        assertThat(load(initial)).isEmpty();

        assertThat(opener.openedTokens()).isEmpty();
        assertThat(opener.closes()).isEqualTo(1);
        assertThat(planning.closes()).isEqualTo(1);
    }

    @Test
    void aRowTheDeserializerSkipsOrExpandsDoesNotEndTheLoad() throws Exception {
        ScriptedPartitionPlanner.Factory planning =
                ScriptedPartitionPlanner.planning("skips", "p0", "p1");
        ScriptedStructStreamOpener opener = opener("skips");
        SpannerFullCacheInputFormat initial =
                format(planning, opener, TimestampBound.strong(), false, new SkipOneDoubleThree());

        // Row 1 emits nothing and opens the load, so the loop must read past it; row 3 emits two.
        assertThat(load(initial)).containsExactly(2L, 3L, 3L);
    }

    /** Emits nothing for id 1 and two records for id 3. */
    private static final class SkipOneDoubleThree
            implements SpannerStructDeserializationSchema<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(com.google.cloud.spanner.Struct row, Collector<RowData> out) {
            long id = row.getLong("id");
            for (int i = 0; i < (id == 1L ? 0 : id == 3L ? 2 : 1); i++) {
                out.collect(GenericRowData.of(id));
            }
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return InternalTypeInfo.of(PHYSICAL.getLogicalType());
        }
    }

    private static ScriptedStructStreamOpener opener(String id) {
        Map<String, List<com.google.cloud.spanner.Struct>> rows = new LinkedHashMap<>();
        rows.put("p0", TestStructs.rows(1L, 2L));
        rows.put("p1", TestStructs.rows(3L));
        return ScriptedStructStreamOpener.over(id, rows);
    }

    private static SpannerException unavailable() {
        return SpannerExceptionFactory.newSpannerException(ErrorCode.UNAVAILABLE, "unavailable");
    }

    private static SpannerFullCacheInputFormat format(
            ScriptedPartitionPlanner.Factory planning,
            ScriptedStructStreamOpener opener,
            TimestampBound bound,
            boolean dataBoost) {
        return format(
                planning,
                opener,
                bound,
                dataBoost,
                new RowDataDeserializationSchema(
                        new StructToRowDataConverter(SCHEMA, null),
                        InternalTypeInfo.of(PHYSICAL.getLogicalType())));
    }

    private static SpannerFullCacheInputFormat format(
            ScriptedPartitionPlanner.Factory planning,
            ScriptedStructStreamOpener opener,
            TimestampBound bound,
            boolean dataBoost,
            SpannerStructDeserializationSchema<RowData> deserializer) {
        SpannerSourceBuilder<RowData> builder =
                SpannerSource.<RowData>builder()
                        .database(TestSources.DATABASE)
                        .readOperation(
                                SpannerReadOperation.read(
                                        "people", KeySet.all(), Collections.singletonList("id")))
                        .deserializer(deserializer)
                        .timestampBound(bound)
                        .dataBoostEnabled(dataBoost)
                        .emulatorEndpoint("localhost:1");
        TestSources.withPlannerFactory(builder, planning);
        TestSources.withOpener(builder, opener);
        @SuppressWarnings("unchecked")
        SpannerBatchReadSource<RowData> source = (SpannerBatchReadSource<RowData>) builder.build();
        return new SpannerFullCacheInputFormat(source.getConfig());
    }

    /** One load the way Flink's loader runs it: a configured copy per split. */
    private static List<Long> load(SpannerFullCacheInputFormat initial) throws Exception {
        List<Long> ids = new ArrayList<>();
        for (GenericInputSplit split : initial.createInputSplits(1)) {
            SpannerFullCacheInputFormat format = copy(initial);
            format.open(split);
            try {
                ids.addAll(drain(format));
            } finally {
                format.close();
            }
        }
        return ids;
    }

    private static SpannerFullCacheInputFormat copy(SpannerFullCacheInputFormat initial)
            throws Exception {
        SpannerFullCacheInputFormat format = InstantiationUtil.clone(initial);
        format.configure(new Configuration());
        return format;
    }

    private static List<Long> drain(SpannerFullCacheInputFormat format) throws Exception {
        List<Long> ids = new ArrayList<>();
        while (!format.reachedEnd()) {
            RowData row = format.nextRecord(null);
            ids.add(row.getLong(0));
        }
        return ids;
    }
}
