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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Applies connector-side table and column filters to decoded data-change records. */
@Internal
public final class SpannerChangeStreamRecordFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Pattern> tableIncludeList;
    private final List<Pattern> tableExcludeList;
    private final List<Pattern> columnIncludeList;
    private final List<Pattern> columnExcludeList;
    private final boolean skipMessagesWithoutChange;

    public static SpannerChangeStreamRecordFilter none() {
        return new SpannerChangeStreamRecordFilter(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                false);
    }

    public SpannerChangeStreamRecordFilter(
            List<Pattern> tableIncludeList,
            List<Pattern> tableExcludeList,
            List<Pattern> columnIncludeList,
            List<Pattern> columnExcludeList,
            boolean skipMessagesWithoutChange) {
        this.tableIncludeList = immutableCopy(tableIncludeList, "tableIncludeList");
        this.tableExcludeList = immutableCopy(tableExcludeList, "tableExcludeList");
        this.columnIncludeList = immutableCopy(columnIncludeList, "columnIncludeList");
        this.columnExcludeList = immutableCopy(columnExcludeList, "columnExcludeList");
        Preconditions.checkArgument(
                this.tableIncludeList.isEmpty() || this.tableExcludeList.isEmpty(),
                "tableIncludeList and tableExcludeList must not both be set");
        Preconditions.checkArgument(
                this.columnIncludeList.isEmpty() || this.columnExcludeList.isEmpty(),
                "columnIncludeList and columnExcludeList must not both be set");
        this.skipMessagesWithoutChange = skipMessagesWithoutChange;
    }

    public Result filter(DataChangeRecord record) {
        Preconditions.checkNotNull(record, "record must not be null");
        if (!included(record.getTableName(), tableIncludeList, tableExcludeList)) {
            return Result.tableFiltered();
        }
        if (columnIncludeList.isEmpty() && columnExcludeList.isEmpty()) {
            return Result.deliver(record, 0);
        }

        String tableName = record.getTableName();
        Set<String> primaryKeys = new HashSet<>();
        List<DataChangeRecord.ColumnType> columnTypes = new ArrayList<>();
        long removedOccurrences = 0;
        for (DataChangeRecord.ColumnType columnType : record.getColumnTypes()) {
            if (columnType.isPrimaryKey()) {
                primaryKeys.add(columnType.getName());
            }
            if (columnType.isPrimaryKey() || includedColumn(tableName, columnType.getName())) {
                columnTypes.add(columnType);
            } else {
                removedOccurrences++;
            }
        }

        List<Mod> mods = new ArrayList<>(record.getMods().size());
        long originalNonKeyOccurrences = 0;
        long retainedNonKeyOccurrences = 0;
        for (Mod mod : record.getMods()) {
            ValueProjection newValues =
                    projectValue(mod.getNewValuesJson().orElse(null), tableName, primaryKeys);
            ValueProjection oldValues =
                    projectValue(mod.getOldValuesJson().orElse(null), tableName, primaryKeys);
            removedOccurrences += newValues.removedOccurrences + oldValues.removedOccurrences;
            originalNonKeyOccurrences +=
                    newValues.originalNonKeyOccurrences + oldValues.originalNonKeyOccurrences;
            retainedNonKeyOccurrences +=
                    newValues.retainedNonKeyOccurrences + oldValues.retainedNonKeyOccurrences;
            mods.add(new Mod(mod.getKeysJson(), newValues.json, oldValues.json));
        }

        if (skipMessagesWithoutChange
                && originalNonKeyOccurrences > 0
                && retainedNonKeyOccurrences == 0) {
            return Result.skippedWithoutChange();
        }
        if (removedOccurrences == 0) {
            return Result.deliver(record, 0);
        }
        return Result.deliver(copy(record, columnTypes, mods), removedOccurrences);
    }

    private boolean includedColumn(String tableName, String columnName) {
        return included(tableName + "." + columnName, columnIncludeList, columnExcludeList);
    }

    private ValueProjection projectValue(
            @Nullable String json, String tableName, Set<String> primaryKeys) {
        if (json == null) {
            return ValueProjection.absent();
        }
        JsonElement value = JsonParser.parseString(json);
        if (value.isJsonNull()) {
            return ValueProjection.explicitNull();
        }
        Preconditions.checkArgument(
                value.isJsonObject(),
                "Spanner Change Streams mod values must be JSON objects or null");

        JsonObject retained = new JsonObject();
        long originalNonKeyOccurrences = 0;
        long retainedNonKeyOccurrences = 0;
        long removedOccurrences = 0;
        for (Map.Entry<String, JsonElement> entry : value.getAsJsonObject().entrySet()) {
            boolean primaryKey = primaryKeys.contains(entry.getKey());
            if (!primaryKey) {
                originalNonKeyOccurrences++;
            }
            if (primaryKey || includedColumn(tableName, entry.getKey())) {
                retained.add(entry.getKey(), entry.getValue());
                if (!primaryKey) {
                    retainedNonKeyOccurrences++;
                }
            } else {
                removedOccurrences++;
            }
        }
        return new ValueProjection(
                retained.toString(),
                originalNonKeyOccurrences,
                retainedNonKeyOccurrences,
                removedOccurrences);
    }

    private static boolean included(
            String identifier, List<Pattern> includes, List<Pattern> excludes) {
        if (!includes.isEmpty()) {
            for (Pattern pattern : includes) {
                if (pattern.matcher(identifier).matches()) {
                    return true;
                }
            }
            return false;
        }
        for (Pattern pattern : excludes) {
            if (pattern.matcher(identifier).matches()) {
                return false;
            }
        }
        return true;
    }

    private static DataChangeRecord copy(
            DataChangeRecord record,
            List<DataChangeRecord.ColumnType> columnTypes,
            List<Mod> mods) {
        return new DataChangeRecord(
                record.getCommitTimestamp(),
                record.getRecordSequence(),
                record.getServerTransactionId(),
                record.isLastRecordInTransactionInPartition(),
                record.getTableName(),
                columnTypes,
                mods,
                record.getModType(),
                record.getValueCaptureType(),
                record.getNumberOfRecordsInTransaction(),
                record.getNumberOfPartitionsInTransaction(),
                record.getTransactionTag(),
                record.isSystemTransaction());
    }

    private static List<Pattern> immutableCopy(List<Pattern> patterns, String name) {
        Preconditions.checkNotNull(patterns, name + " must not be null");
        Preconditions.checkArgument(!patterns.contains(null), name + " must not contain null");
        return Collections.unmodifiableList(new ArrayList<>(patterns));
    }

    /** Result of applying table and column filters to one data-change record. */
    @Internal
    public static final class Result {

        public enum Disposition {
            DELIVER,
            TABLE_FILTERED,
            SKIPPED_WITHOUT_CHANGE
        }

        private final Disposition disposition;
        @Nullable private final DataChangeRecord record;
        private final long removedColumnOccurrences;

        private Result(
                Disposition disposition,
                @Nullable DataChangeRecord record,
                long removedColumnOccurrences) {
            this.disposition = disposition;
            this.record = record;
            this.removedColumnOccurrences = removedColumnOccurrences;
        }

        private static Result deliver(DataChangeRecord record, long removedColumnOccurrences) {
            return new Result(Disposition.DELIVER, record, removedColumnOccurrences);
        }

        private static Result tableFiltered() {
            return new Result(Disposition.TABLE_FILTERED, null, 0);
        }

        private static Result skippedWithoutChange() {
            return new Result(Disposition.SKIPPED_WITHOUT_CHANGE, null, 0);
        }

        public Disposition getDisposition() {
            return disposition;
        }

        public DataChangeRecord getRecord() {
            return Preconditions.checkNotNull(record, "A filtered result has no record");
        }

        public long getRemovedColumnOccurrences() {
            return removedColumnOccurrences;
        }
    }

    private static final class ValueProjection {

        @Nullable private final String json;
        private final long originalNonKeyOccurrences;
        private final long retainedNonKeyOccurrences;
        private final long removedOccurrences;

        private ValueProjection(
                @Nullable String json,
                long originalNonKeyOccurrences,
                long retainedNonKeyOccurrences,
                long removedOccurrences) {
            this.json = json;
            this.originalNonKeyOccurrences = originalNonKeyOccurrences;
            this.retainedNonKeyOccurrences = retainedNonKeyOccurrences;
            this.removedOccurrences = removedOccurrences;
        }

        private static ValueProjection absent() {
            return new ValueProjection(null, 0, 0, 0);
        }

        private static ValueProjection explicitNull() {
            return new ValueProjection("null", 0, 0, 0);
        }
    }
}
