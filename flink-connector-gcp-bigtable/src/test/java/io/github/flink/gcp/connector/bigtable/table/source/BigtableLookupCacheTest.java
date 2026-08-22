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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.functions.table.lookup.CachingAsyncLookupFunction;
import org.apache.flink.table.runtime.functions.table.lookup.CachingLookupFunction;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.CacheLoader;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.LookupFullCache;
import org.apache.flink.table.types.logical.RowType;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class BigtableLookupCacheTest {

    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf1",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q", DataTypes.STRING()))))
                                    .getLogicalType());

    @Test
    void synchronousPartialCacheAvoidsRepeatedHitsAndCacheableMisses() throws Exception {
        CountingRowLookup lookup = new CountingRowLookup("hit");
        CachingLookupFunction function =
                new CachingLookupFunction(
                        partialCache(10, true),
                        new BigtableRowDataLookupFunction(SCHEMA, null, "null", 0, lookup));
        function.open(functionContext());
        try {
            GenericRowData hit = key("hit");
            GenericRowData miss = key("miss");

            assertThat(function.lookup(hit)).hasSize(1);
            assertThat(function.lookup(hit)).hasSize(1);
            assertThat(function.lookup(miss)).isEmpty();
            assertThat(function.lookup(miss)).isEmpty();

            assertThat(lookup.reads).isEqualTo(2);
        } finally {
            function.close();
        }
    }

    @Test
    void asynchronousPartialCacheAvoidsRepeatedHitsAndCacheableMisses() throws Exception {
        CountingRowLookup lookup = new CountingRowLookup("hit");
        CachingAsyncLookupFunction function =
                new CachingAsyncLookupFunction(
                        partialCache(10, true),
                        new BigtableRowDataAsyncLookupFunction(SCHEMA, null, "null", 0, lookup));
        function.open(functionContext());
        try {
            GenericRowData hit = key("hit");
            GenericRowData miss = key("miss");

            assertThat(function.asyncLookup(hit).join()).hasSize(1);
            assertThat(function.asyncLookup(hit).join()).hasSize(1);
            assertThat(function.asyncLookup(miss).join()).isEmpty();
            assertThat(function.asyncLookup(miss).join()).isEmpty();

            assertThat(lookup.reads).isEqualTo(2);
        } finally {
            function.close();
        }
    }

    @Test
    void synchronousPartialCacheDelegatesAfterSizeEvictionAndForUncachedMisses() throws Exception {
        CountingRowLookup lookup = new CountingRowLookup("first", "second");
        CachingLookupFunction function =
                new CachingLookupFunction(
                        partialCache(1, false),
                        new BigtableRowDataLookupFunction(SCHEMA, null, "null", 0, lookup));
        function.open(functionContext());
        try {
            GenericRowData first = key("first");
            GenericRowData second = key("second");
            GenericRowData miss = key("miss");

            assertThat(function.lookup(first)).hasSize(1);
            assertThat(function.lookup(second)).hasSize(1);
            assertThat(function.lookup(first)).hasSize(1);
            assertThat(function.lookup(miss)).isEmpty();
            assertThat(function.lookup(miss)).isEmpty();

            assertThat(lookup.reads).isEqualTo(5);
        } finally {
            function.close();
        }
    }

    @Test
    void asynchronousPartialCacheDelegatesAfterSizeEvictionAndForUncachedMisses() throws Exception {
        CountingRowLookup lookup = new CountingRowLookup("first", "second");
        CachingAsyncLookupFunction function =
                new CachingAsyncLookupFunction(
                        partialCache(1, false),
                        new BigtableRowDataAsyncLookupFunction(SCHEMA, null, "null", 0, lookup));
        function.open(functionContext());
        try {
            GenericRowData first = key("first");
            GenericRowData second = key("second");
            GenericRowData miss = key("miss");

            assertThat(function.asyncLookup(first).join()).hasSize(1);
            assertThat(function.asyncLookup(second).join()).hasSize(1);
            assertThat(function.asyncLookup(first).join()).hasSize(1);
            assertThat(function.asyncLookup(miss).join()).isEmpty();
            assertThat(function.asyncLookup(miss).join()).isEmpty();

            assertThat(lookup.reads).isEqualTo(5);
        } finally {
            function.close();
        }
    }

    @Test
    void fullCacheReloadReplacesTheVisibleSnapshot() throws Exception {
        GenericRowData firstKey = key("first");
        GenericRowData secondKey = key("second");
        ScriptedRowStreamOpener rowStreams =
                new ScriptedRowStreamOpener()
                        .snapshot(row("first", "old"))
                        .snapshot(row("second", "new"));
        BigtableFullCacheInputFormat inputFormat =
                new BigtableFullCacheInputFormat(
                        TableDestination.of("project", "instance", "table"),
                        SCHEMA,
                        null,
                        "null",
                        Filters.FILTERS.family().exactMatch("cf1"),
                        Collections.singletonList(ByteStringRange.unbounded()),
                        null,
                        null,
                        null,
                        rowStreams);
        ScriptedCacheLoader loader = new ScriptedCacheLoader(inputFormat);
        ControllableReloadTrigger trigger = new ControllableReloadTrigger();
        LookupFullCache cache = new LookupFullCache(loader, trigger);
        cache.setUserCodeClassLoader(getClass().getClassLoader());
        cache.open(UnregisteredMetricsGroup.createCacheMetricGroup());
        try {
            assertThat(cachedValue(cache, firstKey)).isEqualTo("old");
            assertThat(cache.getIfPresent(secondKey)).isEmpty();

            trigger.reload().join();

            assertThat(cache.getIfPresent(firstKey)).isEmpty();
            assertThat(cachedValue(cache, secondKey)).isEqualTo("new");
            assertThat(loader.loads).isEqualTo(2);
        } finally {
            cache.close();
        }
    }

    private static DefaultLookupCache partialCache(long maximumSize, boolean cacheMissingKey) {
        return DefaultLookupCache.newBuilder()
                .maximumSize(maximumSize)
                .cacheMissingKey(cacheMissingKey)
                .build();
    }

    private static FunctionContext functionContext() {
        RuntimeContext runtimeContext =
                (RuntimeContext)
                        Proxy.newProxyInstance(
                                RuntimeContext.class.getClassLoader(),
                                new Class<?>[] {RuntimeContext.class},
                                (proxy, method, arguments) -> {
                                    switch (method.getName()) {
                                        case "getMetricGroup":
                                            return UnregisteredMetricsGroup
                                                    .createOperatorMetricGroup();
                                        case "getUserCodeClassLoader":
                                            return BigtableLookupCacheTest.class.getClassLoader();
                                        case "getGlobalJobParameters":
                                            return Collections.emptyMap();
                                        case "isObjectReuseEnabled":
                                            return false;
                                        case "toString":
                                            return "BigtableLookupCacheTestRuntimeContext";
                                        default:
                                            throw new UnsupportedOperationException(
                                                    "Unexpected RuntimeContext call: "
                                                            + method.getName());
                                    }
                                });
        return new FunctionContext(runtimeContext);
    }

    private static GenericRowData key(String key) {
        return GenericRowData.of(StringData.fromString(key));
    }

    private static String cachedValue(LookupFullCache cache, RowData key) {
        Collection<RowData> rows = cache.getIfPresent(key);
        assertThat(rows).hasSize(1);
        return rows.iterator().next().getRow(1, 1).getString(0).toString();
    }

    private static Row row(String key, String value) {
        RowCell cell =
                RowCell.create(
                        "cf1",
                        ByteString.copyFromUtf8("q"),
                        1_000L,
                        Collections.emptyList(),
                        ByteString.copyFromUtf8(value));
        return Row.create(ByteString.copyFromUtf8(key), Collections.singletonList(cell));
    }

    private static final class CountingRowLookup implements BigtableRowLookup {

        private static final long serialVersionUID = 1L;

        private final Set<String> hits = new HashSet<>();
        private int reads;

        private CountingRowLookup(String... hits) {
            Collections.addAll(this.hits, hits);
        }

        @Override
        public void open() {}

        @Override
        @Nullable
        public Row read(ByteString rowKey) {
            reads++;
            String key = rowKey.toStringUtf8();
            return hits.contains(key) ? row(key, "value") : null;
        }

        @Override
        public ApiFuture<Row> readAsync(ByteString rowKey) {
            return ApiFutures.immediateFuture(read(rowKey));
        }

        @Override
        public void close() {}
    }

    private static final class ControllableReloadTrigger implements CacheReloadTrigger {

        private static final long serialVersionUID = 1L;

        private transient Context context;

        @Override
        public void open(Context context) {
            this.context = context;
            context.triggerReload();
        }

        private CompletableFuture<Void> reload() {
            return context.triggerReload();
        }

        @Override
        public void close() {}
    }

    private static final class ScriptedRowStreamOpener
            implements BigtableFullCacheInputFormat.RowStreamOpener {

        private static final long serialVersionUID = 1L;

        private final Deque<List<Row>> snapshots = new ArrayDeque<>();

        private ScriptedRowStreamOpener snapshot(Row... rows) {
            snapshots.add(java.util.Arrays.asList(rows));
            return this;
        }

        @Override
        public Iterator<Row> open(ByteStringRange range) {
            return snapshots.removeFirst().iterator();
        }
    }

    private static final class ScriptedCacheLoader extends CacheLoader {

        private static final long serialVersionUID = 1L;

        private final BigtableFullCacheInputFormat inputFormat;
        private int loads;

        private ScriptedCacheLoader(BigtableFullCacheInputFormat inputFormat) {
            this.inputFormat = inputFormat;
        }

        @Override
        protected boolean updateCache() throws Exception {
            ConcurrentHashMap<RowData, Collection<RowData>> next = new ConcurrentHashMap<>();
            inputFormat.open(new GenericInputSplit(0, 1));
            try {
                while (!inputFormat.reachedEnd()) {
                    RowData row = inputFormat.nextRecord(null);
                    next.put(key(row.getString(0).toString()), Collections.singletonList(row));
                }
                cache = next;
                loads++;
                return true;
            } finally {
                inputFormat.close();
            }
        }
    }
}
