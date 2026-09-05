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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.RowRanges;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalFilter;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalMutation;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalRequest;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Turns a changelog row into one Bigtable mutation, per the table's {@link BigtableTableSchema}.
 *
 * <p>An insert or an update writes every qualifier the DDL declares — including the ones whose
 * value is null, which become an empty cell, or the {@code null-string-literal} for a character
 * string. Writing them rather than skipping them is what makes the row read back as the DDL says it
 * should: a qualifier left unwritten would keep whatever an earlier version of the row put there. A
 * whole column family that is null writes no cells at all, since there is no value to encode. When
 * the DDL selects writable {@code timestamp} metadata, its non-null value is applied to every cell
 * this row writes; a null value, or no metadata column at all, takes the writer clock this schema
 * stamps itself ({@link WallClock}, ADR-0149). This connector stamps only mutations it builds: a
 * DataStream serializer that builds its own {@code RowMutationEntry} owns its timestamps, and
 * nothing here rewrites one a user handed over.
 *
 * <p>Keep-latest deletes all versions of each written cell immediately before setting it, in the
 * same entry. Null families and undeclared qualifiers remain untouched.
 *
 * <p>A delete removes the entire row, not the declared qualifiers one by one. The row key is the
 * primary key of an upsert sink, so "this key is gone" is what a {@code -D} means here; deleting
 * only the declared cells would leave a row behind made of whatever else was in it.
 *
 * <p>Three rejections, each of which fails the record through the sink's failure handler rather
 * than skipping it:
 *
 * <ul>
 *   <li>An {@code UPDATE_BEFORE} row, which the declared upsert changelog mode means the planner
 *       never sends. Treating it as a delete — which is what falling through to the {@code else}
 *       branch would do — would erase the row an update is about to rewrite.
 *   <li>An absent or empty row key. Bigtable has no such row, so failing here is what keeps the job
 *       honest about what it wrote. Flink's HBase connector drops the record instead, which leaves
 *       an incomplete table under a green job.
 *   <li>A row whose every column family is null, which would produce an entry carrying no mutation
 *       at all. The service answers that with an {@code INVALID_ARGUMENT} naming neither the row
 *       nor the reason, so it is refused here where both can be said. A partial column list in an
 *       {@code INSERT} is the ordinary way to reach it.
 * </ul>
 */
@Internal
final class RowDataSerializationSchema implements BigtableSerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final int rowKeyIndex;
    private final String rowKeyName;
    private final CellValueCodec.FieldEncoder rowKeyEncoder;
    private final Family[] families;
    private final int timestampMetadataIndex;
    private final boolean truncateCellTimestampToMillis;
    private final boolean keepLatest;

    /**
     * Transient and restored in {@link #readObject}, not because it holds lambdas but because it is
     * a field added after 1.0.0. A job graph written by 1.0.0 does not carry it, and a
     * non-transient reference would restore as {@code null} and fail the first writer-clock record
     * on a last-state upgrade — the shape ADR-0125 requires an upgrade path for after the tag.
     * Restoring it unconditionally also means a graph carrying a test clock comes back on the
     * production one, which is the right way round.
     */
    private transient CellClock clock;

    /**
     * Creates the schema from the table's DDL model.
     *
     * @param schema the parsed DDL model
     * @param nullStringLiteral the cell value that stands for a null character string
     * @param metadata the writable metadata the planner selected, in the order it laid the consumed
     *     row out in; the timestamp's index is read out of it here rather than assumed, and only
     *     that index is kept
     * @param truncateCellTimestampToMillis whether explicit timestamps lose their sub-millisecond
     *     part before being sent
     */
    RowDataSerializationSchema(
            BigtableTableSchema schema,
            String nullStringLiteral,
            WritableMetadata[] metadata,
            boolean truncateCellTimestampToMillis) {
        this(schema, nullStringLiteral, metadata, truncateCellTimestampToMillis, false);
    }

    /** Creates a schema that optionally replaces all versions of each written cell. */
    RowDataSerializationSchema(
            BigtableTableSchema schema,
            String nullStringLiteral,
            WritableMetadata[] metadata,
            boolean truncateCellTimestampToMillis,
            boolean keepLatest) {
        this(
                schema,
                nullStringLiteral,
                metadata,
                truncateCellTimestampToMillis,
                keepLatest,
                new WallClock());
    }

    /**
     * The seam the tests use, so the stamped value can be one no wall clock produces.
     *
     * <p>Package-private and not reachable from the factory: production always takes {@link
     * WallClock}. Without it nothing could tell this connector's stamping apart from the client
     * library's, because against google-cloud-bigtable 2.81.0 the two produce identical values —
     * which is this change's point and would otherwise be its blind spot.
     *
     * @param clock the source of the writer-clock cell timestamp
     */
    RowDataSerializationSchema(
            BigtableTableSchema schema,
            String nullStringLiteral,
            WritableMetadata[] metadata,
            boolean truncateCellTimestampToMillis,
            boolean keepLatest,
            CellClock clock) {
        this.clock = clock;
        this.keepLatest = keepLatest;
        this.rowKeyIndex = schema.getRowKeyIndex();
        this.rowKeyName = schema.getRowKeyName();
        this.timestampMetadataIndex =
                WritableMetadata.TIMESTAMP.position(schema.getFieldCount(), metadata);
        this.truncateCellTimestampToMillis = truncateCellTimestampToMillis;
        // The plain encoder, not the nullable one: a null row key is rejected below rather than
        // encoded as an empty cell.
        this.rowKeyEncoder = CellValueCodec.encoder(schema.getRowKeyType());
        byte[] nullStringBytes = nullStringLiteral.getBytes(StandardCharsets.UTF_8);
        List<BigtableTableSchema.Family> declared = schema.getFamilies();
        this.families = new Family[declared.size()];
        for (int i = 0; i < declared.size(); i++) {
            families[i] = new Family(declared.get(i), nullStringBytes);
        }
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        this.clock = new WallClock();
    }

    /**
     * The source of the cell timestamp for a row that selected no writable {@code timestamp}.
     *
     * <p>A named type rather than a lambda: this schema crosses the job graph, and {@code
     * crossesTheJobGraphWithoutItsCodecLambdas} pins that it carries no {@code SerializedLambda} —
     * a synthetic name the compiler picks is rebound on restore, which this module has already been
     * bitten by once.
     */
    interface CellClock extends Serializable {

        /**
         * Returns the timestamp to stamp one cell with.
         *
         * @return microseconds since the epoch, a multiple of 1000
         */
        long micros();
    }

    /**
     * The production clock: the current millisecond, expressed in microseconds.
     *
     * <p>The client library's own writer clock would do, and did until google-cloud-bigtable 2.82.0
     * changed {@code setCell(family, qualifier, value)} from {@code currentTimeMillis() * 1000} to
     * a microsecond {@code Instant.now()} marked {@code CLIENT_AUTO_GENERATED}. The service reads
     * that marker and truncates, so both spellings store the same cell — measured 2026-09-03
     * against a real instance. Stamping it here rather than leaving it to the client keeps three
     * things this connector would otherwise inherit from a dependency's clock: the value on the
     * wire does not change under a client upgrade, it is the same value this connector wrote before
     * 2.82.0, and it is expressible on every server that speaks the API.
     *
     * <p>The cost is the sub-millisecond part, which a Bigtable table cannot store anyway: cell
     * granularity is milliseconds. A table created with {@code MICROS} granularity could store it,
     * and this connector neither creates one nor offers a DDL option asking for one — writing to a
     * pre-existing one loses precision the client's own clock would have kept (ADR-0149).
     */
    static final class WallClock implements CellClock {

        private static final long serialVersionUID = 1L;

        @Override
        public long micros() {
            return System.currentTimeMillis() * 1_000L;
        }
    }

    @Override
    public RowMutationEntry serialize(RowData element, SinkWriter.Context context)
            throws IOException {
        RowKind kind = element.getRowKind();
        if (kind == RowKind.UPDATE_BEFORE) {
            throw new IOException(
                    "An UPDATE_BEFORE row reached the Bigtable table sink, which declares an"
                            + " upsert changelog and so is never sent one. Writing it would delete"
                            + " the row the following UPDATE_AFTER rewrites.");
        }
        ByteString key = rowKey(element);
        RowMutationEntry entry = RowMutationEntry.create(key);
        if (kind == RowKind.DELETE) {
            return entry.deleteRow();
        }
        writeCells(
                element,
                key,
                (family, qualifier, timestamp, value) -> {
                    if (keepLatest) {
                        entry.deleteCells(family, qualifier);
                    }
                    entry.setCell(family, qualifier, timestamp, value);
                });
        return entry;
    }

    ConditionalRequest insertIfAbsent(RowData element) throws IOException {
        if (element.getRowKind() != RowKind.INSERT) {
            throw new IOException(
                    "Bigtable sink.write-mode=insert-if-absent accepts INSERT rows only; received "
                            + element.getRowKind()
                            + ".");
        }
        ByteString key = rowKey(element);
        List<ConditionalMutation> cells = new ArrayList<>();
        writeCells(
                element,
                key,
                (family, qualifier, timestamp, value) ->
                        cells.add(
                                ConditionalMutation.setCell(family, qualifier, timestamp, value)));
        return ConditionalRequest.of(key, ConditionalFilter.rowExists(), List.of(), cells);
    }

    private void writeCells(RowData element, ByteString key, CellConsumer consumer)
            throws IOException {
        boolean hasExplicitTimestamp =
                timestampMetadataIndex >= 0 && !element.isNullAt(timestampMetadataIndex);
        long timestampMicros = hasExplicitTimestamp ? timestampMicros(element) : 0L;
        int written = 0;
        for (Family family : families) {
            if (element.isNullAt(family.index)) {
                continue;
            }
            RowData cells = element.getRow(family.index, family.qualifiers.length);
            for (int i = 0; i < family.qualifiers.length; i++) {
                ByteString value = ByteString.copyFrom(family.encoders[i].encode(cells, i));
                consumer.setCell(
                        family.name,
                        family.qualifiers[i],
                        hasExplicitTimestamp ? timestampMicros : clock.micros(),
                        value);
            }
            written += family.qualifiers.length;
        }
        if (written == 0) {
            throw new IOException(
                    String.format(
                            "Every column family of the row with key '%s' is null, so the mutation"
                                    + " would carry no cell. Bigtable rejects such a request with"
                                    + " INVALID_ARGUMENT and a message that names neither the row"
                                    + " nor the reason, so it is refused here instead. A row with"
                                    + " nothing but a key is not a Bigtable row; write at least"
                                    + " one column family, or filter the record out upstream.",
                            RowRanges.format(key)));
        }
    }

    @FunctionalInterface
    private interface CellConsumer {
        void setCell(String family, ByteString qualifier, long timestamp, ByteString value);
    }

    private long timestampMicros(RowData element) throws IOException {
        TimestampData timestamp =
                element.getTimestamp(timestampMetadataIndex, WritableMetadata.TIMESTAMP_PRECISION);
        if (timestamp.getMillisecond() < 0) {
            throw new IOException(
                    "The 'timestamp' metadata value must not be before the Unix epoch. Use NULL"
                            + " for the writer clock; negative metadata cannot select server time.");
        }
        try {
            long millis = Math.multiplyExact(timestamp.getMillisecond(), 1_000L);
            if (truncateCellTimestampToMillis) {
                return millis;
            }
            return Math.addExact(millis, timestamp.getNanoOfMillisecond() / 1_000L);
        } catch (ArithmeticException e) {
            throw new IOException(
                    "The 'timestamp' metadata value is outside the range of epoch microseconds.",
                    e);
        }
    }

    private ByteString rowKey(RowData element) throws IOException {
        if (element.isNullAt(rowKeyIndex)) {
            throw new IOException(
                    String.format(
                            "The row-key column '%s' is null. Bigtable has no row without a key.",
                            rowKeyName));
        }
        ByteString key = ByteString.copyFrom(rowKeyEncoder.encode(element, rowKeyIndex));
        if (key.isEmpty()) {
            throw new IOException(
                    String.format(
                            "The row-key column '%s' encodes to zero bytes. Bigtable has no row"
                                    + " with an empty key.",
                            rowKeyName));
        }
        return key;
    }

    /** One column family's qualifiers and their encoders, resolved once. */
    private static final class Family implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String name;
        private final int index;
        private final ByteString[] qualifiers;
        private final CellValueCodec.FieldEncoder[] encoders;

        private Family(BigtableTableSchema.Family family, byte[] nullStringBytes) {
            this.name = family.getName();
            this.index = family.getIndex();
            List<BigtableTableSchema.Qualifier> declared = family.getQualifiers();
            this.qualifiers = new ByteString[declared.size()];
            this.encoders = new CellValueCodec.FieldEncoder[declared.size()];
            for (int i = 0; i < declared.size(); i++) {
                BigtableTableSchema.Qualifier qualifier = declared.get(i);
                // Encoded once here rather than per record: the SDK's String overload would
                // re-encode the same qualifier for every cell of every row.
                qualifiers[i] = ByteString.copyFromUtf8(qualifier.getName());
                encoders[i] = CellValueCodec.nullableEncoder(qualifier.getType(), nullStringBytes);
            }
        }
    }
}
