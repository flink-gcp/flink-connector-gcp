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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigtable.data.v2.models.DefaultRowAdapter;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowAdapter;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;

import java.util.List;

/**
 * Materialises SDK rows while measuring their decoded wire content without traversing them twice.
 */
@Internal
public final class MeasuringRowAdapter implements RowAdapter<MeasuredRow> {

    private final DefaultRowAdapter delegate = new DefaultRowAdapter();

    @Override
    public RowBuilder<MeasuredRow> createRowBuilder() {
        return new Builder(delegate.createRowBuilder());
    }

    @Override
    public boolean isScanMarkerRow(MeasuredRow row) {
        return delegate.isScanMarkerRow(row.row());
    }

    @Override
    public ByteString getKey(MeasuredRow row) {
        return delegate.getKey(row.row());
    }

    private static final class Builder implements RowBuilder<MeasuredRow> {

        private final RowBuilder<Row> delegate;
        private long estimatedBytes;

        private Builder(RowBuilder<Row> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void startRow(ByteString key) {
            delegate.startRow(key);
            estimatedBytes = CodedOutputStream.computeBytesSizeNoTag(key);
        }

        @Override
        public void startCell(
                String family,
                ByteString qualifier,
                long timestamp,
                List<String> labels,
                long size) {
            delegate.startCell(family, qualifier, timestamp, labels, size);
            add(CodedOutputStream.computeStringSizeNoTag(family));
            add(CodedOutputStream.computeBytesSizeNoTag(qualifier));
            add(CodedOutputStream.computeInt64SizeNoTag(timestamp));
            for (String label : labels) {
                add(CodedOutputStream.computeStringSizeNoTag(label));
            }
            add(CodedOutputStream.computeUInt64SizeNoTag(size));
            add(size);
        }

        @Override
        public void cellValue(ByteString value) {
            delegate.cellValue(value);
        }

        @Override
        public void finishCell() {
            delegate.finishCell();
        }

        @Override
        public MeasuredRow finishRow() {
            return new MeasuredRow(delegate.finishRow(), Math.max(1, estimatedBytes));
        }

        @Override
        public void reset() {
            delegate.reset();
            estimatedBytes = 0;
        }

        @Override
        public MeasuredRow createScanMarkerRow(ByteString key) {
            return new MeasuredRow(
                    delegate.createScanMarkerRow(key),
                    Math.max(1, CodedOutputStream.computeBytesSizeNoTag(key)));
        }

        private void add(long value) {
            if (value > Long.MAX_VALUE - estimatedBytes) {
                estimatedBytes = Long.MAX_VALUE;
            } else {
                estimatedBytes += value;
            }
        }
    }
}
