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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.source.BigtableSource;
import io.github.flink.gcp.connector.bigtable.source.BigtableSourceBuilder;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The {@code bigtable} table source: a bounded scan over the DataStream {@link BigtableSource}.
 *
 * <p>Deliberately takes no {@code ReadableConfig}, for the reason {@code BigtableDynamicSink}
 * records: a DDL option becomes a value in the factory and nowhere else.
 *
 * <p><b>Projection pushdown is top-level family pruning, served server-side.</b> Nested projection
 * is not supported, so a projection retains whole columns: the row key or whole column families.
 * The retained families become a family filter — unread families never leave the server — and a
 * projection retaining <em>no</em> family becomes the keys-only chain {@link
 * FamilyProjectionFilter} builds, because Bigtable has no row without a cell. The filter is applied
 * whether or not a projection was pushed: an unprojected scan retains every declared family, which
 * keeps families the physical table has but the DDL does not declare off the wire, and makes the
 * documented failure mode — a filter naming a column family the table lacks fails the read with
 * {@code NOT_FOUND} — the same in both cases.
 *
 * <p>The source reports the produced type it was handed, never {@code toPhysicalRowDataType()}:
 * after a projection the two differ, and the runtime rows are built to the projected shape.
 */
@Internal
public final class BigtableDynamicSource implements ScanTableSource, SupportsProjectionPushDown {

    private final BigtableTableSchema schema;
    private final TableDestination destination;
    private final String nullStringLiteral;
    @Nullable private final String appProfileId;
    private final List<String> prefixes;
    @Nullable private final String rangeStartClosed;
    @Nullable private final String rangeEndOpen;
    @Nullable private final String emulatorEndpoint;
    @Nullable private final Integer parallelism;

    // The projection state, mutated by applyProjection and carried by copy().
    private DataType producedDataType;
    @Nullable private int[] projectedFields;

    private BigtableDynamicSource(Builder builder) {
        this.schema = Preconditions.checkNotNull(builder.schema, "schema must not be null");
        this.destination =
                Preconditions.checkNotNull(builder.destination, "destination must not be null");
        this.nullStringLiteral =
                Preconditions.checkNotNull(
                        builder.nullStringLiteral, "nullStringLiteral must not be null");
        this.appProfileId = builder.appProfileId;
        this.prefixes = builder.prefixes;
        this.rangeStartClosed = builder.rangeStartClosed;
        this.rangeEndOpen = builder.rangeEndOpen;
        this.emulatorEndpoint = builder.emulatorEndpoint;
        this.parallelism = builder.parallelism;
        this.producedDataType =
                Preconditions.checkNotNull(
                        builder.producedDataType, "producedDataType must not be null");
    }

    /**
     * Returns a builder for this source.
     *
     * @return a builder with nothing set
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public boolean supportsNestedProjection() {
        // A retained family always reads as its full declared ROW. Qualifier-level pruning is a
        // filter Bigtable could serve, but a nested projection also reshapes the produced ROW
        // type, which the converter would then have to mirror; deferred until asked for.
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        // With nested projection unsupported, every inner array has length one and names a
        // top-level physical column.
        int[] fields = new int[projectedFields.length];
        for (int i = 0; i < projectedFields.length; i++) {
            fields[i] = projectedFields[i][0];
        }
        this.projectedFields = fields;
        this.producedDataType = producedDataType;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        // The InternalTypeInfo the planner would have built, reached through a PublicEvolving
        // interface — what keeps flink-table-runtime off this module's dependencies.
        TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(producedDataType);
        RowToRowDataConverter converter =
                new RowToRowDataConverter(schema, projectedFields, nullStringLiteral);
        BigtableSourceBuilder<RowData> builder =
                BigtableSource.<RowData>builder()
                        .table(destination)
                        .deserializer(new RowDataDeserializationSchema(converter, producedTypeInfo))
                        .filter(FamilyProjectionFilter.of(retainedFamilies()));
        for (String prefix : prefixes) {
            builder.prefix(prefix);
        }
        if (rangeStartClosed != null || rangeEndOpen != null) {
            // From unbounded(), so a one-sided bound is expressible; the factory has already
            // rejected an empty-string bound, which the client would widen to the whole table.
            ByteStringRange range = ByteStringRange.unbounded();
            if (rangeStartClosed != null) {
                range.startClosed(rangeStartClosed);
            }
            if (rangeEndOpen != null) {
                range.endOpen(rangeEndOpen);
            }
            builder.rowRange(range);
        }
        if (appProfileId != null) {
            builder.appProfileId(appProfileId);
        }
        if (emulatorEndpoint != null) {
            builder.emulatorEndpoint(emulatorEndpoint);
        }
        Source<RowData, ?, ?> source = builder.build();
        return SourceProvider.of(source, parallelism);
    }

    /** The declared families the projection retains, in DDL order; all of them unprojected. */
    private List<String> retainedFamilies() {
        List<String> names = new ArrayList<>();
        for (BigtableTableSchema.Family family : schema.getFamilies()) {
            if (projectedFields == null || contains(projectedFields, family.getIndex())) {
                names.add(family.getName());
            }
        }
        return names;
    }

    private static boolean contains(int[] fields, int index) {
        for (int field : fields) {
            if (field == index) {
                return true;
            }
        }
        return false;
    }

    @Override
    public DynamicTableSource copy() {
        BigtableDynamicSource copy =
                builder()
                        .schema(schema)
                        .destination(destination)
                        .nullStringLiteral(nullStringLiteral)
                        .appProfileId(appProfileId)
                        .prefixes(prefixes)
                        .rangeStartClosed(rangeStartClosed)
                        .rangeEndOpen(rangeEndOpen)
                        .emulatorEndpoint(emulatorEndpoint)
                        .parallelism(parallelism)
                        .producedDataType(producedDataType)
                        .build();
        copy.projectedFields = projectedFields;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Bigtable table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BigtableDynamicSource that = (BigtableDynamicSource) o;
        return schema.equals(that.schema)
                && destination.equals(that.destination)
                && nullStringLiteral.equals(that.nullStringLiteral)
                && Objects.equals(appProfileId, that.appProfileId)
                && prefixes.equals(that.prefixes)
                && Objects.equals(rangeStartClosed, that.rangeStartClosed)
                && Objects.equals(rangeEndOpen, that.rangeEndOpen)
                && Objects.equals(emulatorEndpoint, that.emulatorEndpoint)
                && Objects.equals(parallelism, that.parallelism)
                && producedDataType.equals(that.producedDataType)
                && Arrays.equals(projectedFields, that.projectedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema,
                destination,
                nullStringLiteral,
                appProfileId,
                prefixes,
                rangeStartClosed,
                rangeEndOpen,
                emulatorEndpoint,
                parallelism,
                producedDataType,
                Arrays.hashCode(projectedFields));
    }

    /** Collects the source's values, so no caller has to keep a positional list in order. */
    public static final class Builder {

        private BigtableTableSchema schema;
        private TableDestination destination;
        private String nullStringLiteral;
        @Nullable private String appProfileId;
        private List<String> prefixes = Collections.emptyList();
        @Nullable private String rangeStartClosed;
        @Nullable private String rangeEndOpen;
        @Nullable private String emulatorEndpoint;
        @Nullable private Integer parallelism;
        private DataType producedDataType;

        private Builder() {}

        /**
         * @param schema the table's parsed DDL model
         * @return this builder
         */
        public Builder schema(BigtableTableSchema schema) {
            this.schema = schema;
            return this;
        }

        /**
         * @param destination the table to scan
         * @return this builder
         */
        public Builder destination(TableDestination destination) {
            this.destination = destination;
            return this;
        }

        /**
         * @param nullStringLiteral the cell value standing for a null character string
         * @return this builder
         */
        public Builder nullStringLiteral(String nullStringLiteral) {
            this.nullStringLiteral = nullStringLiteral;
            return this;
        }

        /**
         * @param appProfileId the app profile the scan reads under, or {@code null}
         * @return this builder
         */
        public Builder appProfileId(@Nullable String appProfileId) {
            this.appProfileId = appProfileId;
            return this;
        }

        /**
         * @param prefixes the UTF-8 row-key prefixes to scan, possibly empty
         * @return this builder
         */
        public Builder prefixes(List<String> prefixes) {
            this.prefixes = new ArrayList<>(prefixes);
            return this;
        }

        /**
         * @param rangeStartClosed the range's inclusive UTF-8 start key, or {@code null} for open
         * @return this builder
         */
        public Builder rangeStartClosed(@Nullable String rangeStartClosed) {
            this.rangeStartClosed = rangeStartClosed;
            return this;
        }

        /**
         * @param rangeEndOpen the range's exclusive UTF-8 end key, or {@code null} for open
         * @return this builder
         */
        public Builder rangeEndOpen(@Nullable String rangeEndOpen) {
            this.rangeEndOpen = rangeEndOpen;
            return this;
        }

        /**
         * @param emulatorEndpoint the emulator's endpoint, or {@code null} for the real service
         * @return this builder
         */
        public Builder emulatorEndpoint(@Nullable String emulatorEndpoint) {
            this.emulatorEndpoint = emulatorEndpoint;
            return this;
        }

        /**
         * @param parallelism the source parallelism, or {@code null} for the planner's own
         * @return this builder
         */
        public Builder parallelism(@Nullable Integer parallelism) {
            this.parallelism = parallelism;
            return this;
        }

        /**
         * @param producedDataType the row type the scan produces — the physical type from the
         *     factory, or the already-projected type when {@code copy()} rebuilds a source
         * @return this builder
         */
        public Builder producedDataType(DataType producedDataType) {
            this.producedDataType = producedDataType;
            return this;
        }

        /**
         * @return the source
         */
        public BigtableDynamicSource build() {
            return new BigtableDynamicSource(this);
        }
    }
}
