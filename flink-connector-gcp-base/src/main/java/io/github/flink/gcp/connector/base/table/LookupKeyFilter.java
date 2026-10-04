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

package io.github.flink.gcp.connector.base.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** Separates an addressing key from additional equality keys on the produced physical row. */
@Internal
public final class LookupKeyFilter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int[] addressingPositions;
    private final RowData.FieldGetter[] keyGetters;
    private final RowData.FieldGetter[] resultGetters;
    private final KeyValue[] keyValues;
    private final boolean[] additional;

    private LookupKeyFilter(
            int[] addressingPositions,
            int[] resultPositions,
            LogicalType[] keyTypes,
            boolean[] additional) {
        this.addressingPositions = addressingPositions;
        this.additional = additional;
        this.keyGetters = new RowData.FieldGetter[keyTypes.length];
        this.resultGetters = new RowData.FieldGetter[keyTypes.length];
        this.keyValues = new KeyValue[keyTypes.length];
        for (int i = 0; i < keyTypes.length; i++) {
            keyGetters[i] = getter(keyTypes[i], i);
            resultGetters[i] = getter(keyTypes[i], resultPositions[i]);
            keyValues[i] = new KeyValue(keyTypes[i]);
        }
    }

    /**
     * Validates top-level keys, preserving the addressing fields' declared order. Physical indexes
     * map produced columns back to the original schema; metadata is excluded.
     */
    public static LookupKeyFilter of(
            RowType producedType,
            int[][] keys,
            @Nullable int[] physicalIndexes,
            int[] addressingFields,
            boolean allowRows,
            String requirement) {
        int arity = physicalIndexes == null ? producedType.getFieldCount() : physicalIndexes.length;
        int[] addressing = new int[addressingFields.length];
        Arrays.fill(addressing, -1);
        int[] results = new int[keys.length];
        LogicalType[] types = new LogicalType[keys.length];
        boolean[] extra = new boolean[keys.length];
        boolean[] seen = new boolean[arity];
        if (addressing.length == 0) {
            throw new ValidationException(requirement);
        }
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].length != 1 || keys[i][0] < 0 || keys[i][0] >= arity || seen[keys[i][0]]) {
                throw new ValidationException(requirement);
            }
            int produced = keys[i][0];
            seen[produced] = true;
            results[i] = produced;
            types[i] = producedType.getTypeAt(produced);
            int physical = physicalIndexes == null ? produced : physicalIndexes[produced];
            int address = -1;
            for (int j = 0; j < addressingFields.length; j++) {
                if (addressingFields[j] == physical) {
                    address = j;
                    break;
                }
            }
            if (address >= 0) {
                addressing[address] = i;
            } else {
                extra[i] = true;
                if (!supported(types[i], allowRows)) {
                    throw new ValidationException(
                            requirement
                                    + " Additional lookup column '"
                                    + producedType.getFieldNames().get(produced)
                                    + "' has unsupported equality type "
                                    + types[i].asSummaryString()
                                    + ".");
                }
            }
        }
        for (int position : addressing) {
            if (position < 0) {
                throw new ValidationException(requirement);
            }
        }
        return new LookupKeyFilter(addressing, results, types, extra);
    }

    private static boolean supported(LogicalType type, boolean allowRows) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return true;
            case ROW:
                return allowRows
                        && type.getChildren().stream().allMatch(child -> supported(child, false));
            default:
                return false;
        }
    }

    /** Positions of the addressing fields in the unmodified planner key row. */
    public int[] addressingPositions() {
        return addressingPositions.clone();
    }

    /** Whether an equality must be evaluated after the point read. */
    public boolean hasAdditionalKeys() {
        for (boolean value : additional) {
            if (value) {
                return true;
            }
        }
        return false;
    }

    /** Wraps a point reader that accepts only its addressing fields, in declared order. */
    public LookupFunction wrap(LookupFunction reader) {
        return hasAdditionalKeys() ? new FilteringLookupFunction(reader, this) : reader;
    }

    /** Wraps an asynchronous point reader, snapshotting its comparison values per invocation. */
    public AsyncLookupFunction wrap(AsyncLookupFunction reader) {
        return hasAdditionalKeys() ? new FilteringAsyncLookupFunction(reader, this) : reader;
    }

    /**
     * Retains Flink's cache lifecycle and policy while normalizing complete equality key tuples.
     */
    public LookupCache wrap(LookupCache cache) {
        return hasAdditionalKeys() ? new EqualityLookupCache(cache, this) : cache;
    }

    private GenericRowData snapshot(RowData row) {
        GenericRowData copy = new GenericRowData(keyValues.length);
        for (int i = 0; i < keyValues.length; i++) {
            Object value = keyGetters[i].getFieldOrNull(row);
            copy.setField(i, keyValues[i].copy(value, additional[i]));
        }
        return copy;
    }

    private static RowData.FieldGetter getter(LogicalType type, int position) {
        // Flink 1.x's getter does not check nulls for a NOT NULL logical type.
        return RowData.createFieldGetter(type.copy(true), position);
    }

    private static boolean containsNull(RowData row) {
        for (int i = 0; i < row.getArity(); i++) {
            if (row.isNullAt(i)) {
                return true;
            }
        }
        return false;
    }

    private RowData addressingKey(RowData key) {
        GenericRowData address = new GenericRowData(addressingPositions.length);
        for (int i = 0; i < addressingPositions.length; i++) {
            address.setField(i, ((GenericRowData) key).getField(addressingPositions[i]));
        }
        return address;
    }

    private Collection<RowData> filter(@Nullable Collection<RowData> rows, RowData key) {
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        List<RowData> matches = new ArrayList<>();
        for (RowData row : rows) {
            boolean match = true;
            for (int i = 0; i < additional.length && match; i++) {
                if (additional[i]) {
                    Object actual = resultGetters[i].getFieldOrNull(row);
                    Object expected = ((GenericRowData) key).getField(i);
                    match = actual != null && keyValues[i].equal(actual, expected);
                }
            }
            if (match) {
                matches.add(row);
            }
        }
        return matches;
    }

    private static final class KeyValue implements Serializable {
        private static final long serialVersionUID = 1L;
        private final LogicalType type;
        private final RowData.FieldGetter[] fields;
        private final KeyValue[] children;

        private KeyValue(LogicalType type) {
            this.type = type;
            List<LogicalType> childTypes =
                    type instanceof RowType ? type.getChildren() : Collections.emptyList();
            fields = new RowData.FieldGetter[childTypes.size()];
            children = new KeyValue[childTypes.size()];
            for (int i = 0; i < children.length; i++) {
                fields[i] = getter(childTypes.get(i), i);
                children[i] = new KeyValue(childTypes.get(i));
            }
        }

        @Nullable
        private Object copy(@Nullable Object value, boolean equality) {
            if (value == null) {
                return null;
            }
            switch (type.getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    return StringData.fromString(value.toString());
                case BINARY:
                case VARBINARY:
                    return ((byte[]) value).clone();
                case FLOAT:
                    return equality && ((Float) value) == 0f ? 0f : value;
                case DOUBLE:
                    return equality && ((Double) value) == 0d ? 0d : value;
                case ROW:
                    GenericRowData copy = new GenericRowData(children.length);
                    for (int i = 0; i < children.length; i++) {
                        copy.setField(
                                i,
                                children[i].copy(
                                        fields[i].getFieldOrNull((RowData) value), equality));
                    }
                    return copy;
                default:
                    return value;
            }
        }

        private boolean equal(@Nullable Object left, @Nullable Object right) {
            if (left == null || right == null) {
                return left == right;
            }
            switch (type.getTypeRoot()) {
                case BINARY:
                case VARBINARY:
                    return Arrays.equals((byte[]) left, (byte[]) right);
                case FLOAT:
                    return ((Float) left).floatValue() == ((Float) right).floatValue();
                case DOUBLE:
                    return ((Double) left).doubleValue() == ((Double) right).doubleValue();
                case ROW:
                    for (int i = 0; i < children.length; i++) {
                        if (!children[i].equal(
                                fields[i].getFieldOrNull((RowData) left),
                                fields[i].getFieldOrNull((RowData) right))) {
                            return false;
                        }
                    }
                    return true;
                default:
                    return left.equals(right);
            }
        }
    }

    /** Public because Flink requires the provided function class to be public. */
    @Internal
    public static final class FilteringLookupFunction extends LookupFunction {
        private static final long serialVersionUID = 1L;
        private final LookupFunction reader;
        private final LookupKeyFilter keys;

        private FilteringLookupFunction(LookupFunction reader, LookupKeyFilter keys) {
            this.reader = reader;
            this.keys = keys;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            reader.open(context);
        }

        @Override
        public Collection<RowData> lookup(RowData keyRow) throws IOException {
            RowData key = keys.snapshot(keyRow);
            return containsNull(key)
                    ? Collections.emptyList()
                    : keys.filter(reader.lookup(keys.addressingKey(key)), key);
        }

        @Override
        public void close() throws Exception {
            reader.close();
        }
    }

    /** Public because Flink requires the provided function class to be public. */
    @Internal
    public static final class FilteringAsyncLookupFunction extends AsyncLookupFunction {
        private static final long serialVersionUID = 1L;
        private final AsyncLookupFunction reader;
        private final LookupKeyFilter keys;

        private FilteringAsyncLookupFunction(AsyncLookupFunction reader, LookupKeyFilter keys) {
            this.reader = reader;
            this.keys = keys;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            reader.open(context);
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
            RowData key = keys.snapshot(keyRow);
            return containsNull(key)
                    ? CompletableFuture.completedFuture(Collections.emptyList())
                    : reader.asyncLookup(keys.addressingKey(key))
                            .thenApply(rows -> keys.filter(rows, key));
        }

        @Override
        public void close() throws Exception {
            reader.close();
        }
    }

    private static final class EqualityLookupCache implements LookupCache {
        private static final long serialVersionUID = 1L;
        private final LookupCache delegate;
        private final LookupKeyFilter keys;

        private EqualityLookupCache(LookupCache delegate, LookupKeyFilter keys) {
            this.delegate = delegate;
            this.keys = keys;
        }

        @Override
        public void open(CacheMetricGroup metricGroup) {
            delegate.open(metricGroup);
        }

        @Override
        @Nullable
        public Collection<RowData> getIfPresent(RowData key) {
            return delegate.getIfPresent(keys.snapshot(key));
        }

        @Override
        public Collection<RowData> put(RowData key, Collection<RowData> value) {
            return delegate.put(keys.snapshot(key), value);
        }

        @Override
        public void invalidate(RowData key) {
            delegate.invalidate(keys.snapshot(key));
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void close() throws Exception {
            delegate.close();
        }
    }
}
