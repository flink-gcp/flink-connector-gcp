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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.table.SpannerTableSchemaConverter;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** Synchronous primary-key lookup producing the projected table row. */
@Internal
public final class SpannerRowDataLookupFunction extends LookupFunction {
    private static final long serialVersionUID = 1L;

    private final SpannerLookupKeyEncoder keyEncoder;
    private final StructToRowDataConverter converter;
    private final int maxRetries;
    private final SpannerRowLookup lookup;
    private final SpannerFilterPushDown.RuntimeState filters;

    SpannerRowDataLookupFunction(
            SpannerDatabase database,
            String table,
            List<String> columns,
            SpannerTableSchemaConverter schema,
            @Nullable int[] projectedFields,
            int[] keyPositions,
            @Nullable String emulatorEndpoint,
            int maxRetries) {
        this(
                database,
                table,
                columns,
                schema,
                projectedFields,
                keyPositions,
                emulatorEndpoint,
                maxRetries,
                SpannerFilterPushDown.State.empty().runtime());
    }

    SpannerRowDataLookupFunction(
            SpannerDatabase database,
            String table,
            List<String> columns,
            SpannerTableSchemaConverter schema,
            @Nullable int[] projectedFields,
            int[] keyPositions,
            @Nullable String emulatorEndpoint,
            int maxRetries,
            SpannerFilterPushDown.RuntimeState filters) {
        this(
                schema,
                projectedFields,
                keyPositions,
                maxRetries,
                new SpannerDatabaseRowLookup(database, table, columns, emulatorEndpoint),
                filters);
    }

    @VisibleForTesting
    SpannerRowDataLookupFunction(
            SpannerTableSchemaConverter schema,
            @Nullable int[] projectedFields,
            int[] keyPositions,
            int maxRetries,
            SpannerRowLookup lookup) {
        this(
                schema,
                projectedFields,
                keyPositions,
                maxRetries,
                lookup,
                SpannerFilterPushDown.State.empty().runtime());
    }

    @VisibleForTesting
    SpannerRowDataLookupFunction(
            SpannerTableSchemaConverter schema,
            @Nullable int[] projectedFields,
            int[] keyPositions,
            int maxRetries,
            SpannerRowLookup lookup,
            SpannerFilterPushDown.RuntimeState filters) {
        this.keyEncoder = new SpannerLookupKeyEncoder(schema, keyPositions);
        this.converter = new StructToRowDataConverter(schema, projectedFields);
        this.maxRetries = maxRetries;
        this.lookup = lookup;
        this.filters = filters;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        lookup.open();
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) {
        if (containsNull(keyRow)) {
            return Collections.emptyList();
        }
        Key key = keyEncoder.encode(keyRow);
        if (!filters.matchesPrimaryKey(key)) {
            return Collections.emptyList();
        }
        for (int retry = 0; ; retry++) {
            try {
                return convert(lookup.read(key));
            } catch (RuntimeException failure) {
                if (retry >= maxRetries || !SpannerLookupErrorClassifier.isTransient(failure)) {
                    throw failure;
                }
            }
        }
    }

    private static boolean containsNull(RowData row) {
        for (int i = 0; i < row.getArity(); i++) {
            if (row.isNullAt(i)) {
                return true;
            }
        }
        return false;
    }

    private Collection<RowData> convert(@Nullable Struct row) {
        return row == null
                ? Collections.emptyList()
                : Collections.singletonList(converter.convert(row));
    }

    @Override
    public void close() throws Exception {
        lookup.close();
    }
}
