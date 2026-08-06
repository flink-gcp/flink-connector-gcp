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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.bigquery.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Maps the {@code sink.table-create.*} options onto {@link TableCreateOptions}.
 *
 * <p>Under the same contract as {@code DefaultStreamOptionsMapper}: every knob is applied through
 * the builder, no default is introduced, and value validation — a blank column name, a fifth
 * clustering column, a non-positive expiration — is left to that builder so a SQL user gets the
 * message a DataStream user gets. Returns {@code null} when the DDL sets no key of the family,
 * which leaves the sink on {@code TableCreateOptions.defaults()} rather than on a copy of it made
 * here.
 *
 * <p><b>These options do not authorize creation</b> — {@code sink.create-disposition} does, and it
 * defaults to {@code create-if-needed}, so the settings alone configure the table an unconfigured
 * DDL already creates. What is rejected is combining them with an explicit {@code create-never},
 * which never creates a table they could apply to.
 *
 * <p>Four rules are owned here rather than left to the builder, each because its message has to
 * name option keys:
 *
 * <ul>
 *   <li>the {@code create-never} contradiction above;
 *   <li>a partitioning field without a granularity, which has <b>no builder backstop at all</b>:
 *       the builder's two {@code timePartitioning} overloads make the pair unrepresentable, so
 *       there is no exception to inherit and this check is the only thing between a DDL and a
 *       silently unpartitioned table;
 *   <li>an expiration without a granularity, which {@code build()} does reject — but naming {@code
 *       timePartitioningExpiration}, a method a SQL user cannot act on;
 *   <li>a partitioning or clustering column BigQuery cannot use — one the table does not declare,
 *       one whose type it cannot partition on, an {@code hour} granularity over a {@code DATE}
 *       column, or a repeated or nested column. Real BigQuery refuses each at creation and the
 *       emulator accepts them all, so without these the failure surfaces only against the service
 *       and only once a row is written. The DataStream API cannot make the check, its schema coming
 *       from the serializer per destination; here the DDL row type is at hand, so it is a check
 *       this layer alone can make. Name matching is case-insensitive, which cannot reject a table
 *       BigQuery would have created whichever way it resolves the name, and is unambiguous because
 *       {@code RowTypeToTableSchemaConverter} already rejects columns differing only by case.
 * </ul>
 */
@Internal
public final class TableCreateOptionsMapper {

    /** Every key of the family, for the "is any of these set?" scan. */
    private static final List<ConfigOption<?>> FAMILY =
            Arrays.asList(
                    BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_TYPE,
                    BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_FIELD,
                    BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_EXPIRATION,
                    BigQueryConnectorOptions.SINK_TABLE_CREATE_CLUSTERED_FIELDS);

    private TableCreateOptionsMapper() {}

    /** Returns the keys of the family that the given configuration sets, in declaration order. */
    private static List<String> presentKeys(ReadableConfig config) {
        List<String> present = new ArrayList<>();
        for (ConfigOption<?> option : FAMILY) {
            if (config.getOptional(option).isPresent()) {
                present.add(option.key());
            }
        }
        return present;
    }

    /**
     * Builds the settings a missing table is created with.
     *
     * @param config the table's options
     * @param rowType the table's physical columns, which a partitioning or clustering column must
     *     be one of, and whose types decide whether BigQuery can partition or cluster on it
     * @return the creation settings, or {@code null} when no {@code sink.table-create.*} option is
     *     set, which leaves a created table unpartitioned and unclustered
     */
    @Nullable
    public static TableCreateOptions map(ReadableConfig config, RowType rowType) {
        List<String> present = presentKeys(config);
        if (present.isEmpty()) {
            return null;
        }
        checkDispositionCreates(config, present);

        Optional<TableCreateOptions.TimePartitioningType> type =
                config.getOptional(
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_TYPE);
        Optional<String> field =
                config.getOptional(
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_FIELD);
        Optional<Duration> expiration =
                config.getOptional(
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_EXPIRATION);
        List<String> clusteredFields =
                config.getOptional(BigQueryConnectorOptions.SINK_TABLE_CREATE_CLUSTERED_FIELDS)
                        .orElse(null);

        if (!type.isPresent()) {
            checkNeedsGranularity(
                    field.isPresent(),
                    BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_FIELD,
                    "there is no partitioning to place the column on");
            checkNeedsGranularity(
                    expiration.isPresent(),
                    BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_EXPIRATION,
                    "there are no partitions to expire");
        }

        field.ifPresent(
                name ->
                        checkPartitioningColumn(
                                name,
                                columnOf(
                                        name,
                                        rowType,
                                        BigQueryConnectorOptions
                                                .SINK_TABLE_CREATE_TIME_PARTITIONING_FIELD),
                                type.orElse(null)));
        if (clusteredFields != null) {
            for (String name : clusteredFields) {
                checkClusteringColumn(
                        name,
                        columnOf(
                                name,
                                rowType,
                                BigQueryConnectorOptions.SINK_TABLE_CREATE_CLUSTERED_FIELDS));
            }
        }

        TableCreateOptions.Builder builder = TableCreateOptions.builder();
        // The one place the two overloads are chosen between: a granularity with a column
        // partitions on that column, a granularity alone partitions on ingestion time.
        type.ifPresent(
                granularity -> {
                    if (field.isPresent()) {
                        builder.timePartitioning(granularity, field.get());
                    } else {
                        builder.timePartitioning(granularity);
                    }
                });
        expiration.ifPresent(builder::timePartitioningExpiration);
        if (clusteredFields != null) {
            builder.clusteredFields(clusteredFields);
        }
        return builder.build();
    }

    private static void checkDispositionCreates(ReadableConfig config, List<String> present) {
        if (config.getOptional(BigQueryConnectorOptions.SINK_CREATE_DISPOSITION).orElse(null)
                == CreateDisposition.CREATE_NEVER) {
            throw new ValidationException(
                    String.format(
                            "Options %s configure a table this sink never creates, because '%s' is"
                                    + " 'create-never'. Remove the options or use"
                                    + " 'create-if-needed'.",
                            present, BigQueryConnectorOptions.SINK_CREATE_DISPOSITION.key()));
        }
    }

    private static void checkNeedsGranularity(
            boolean isSet, ConfigOption<?> option, String because) {
        if (isSet) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' requires '%s': without a granularity %s.",
                            option.key(),
                            BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_TYPE.key(),
                            because));
        }
    }

    /**
     * Resolves the named column against the table's own, or rejects it naming the option key.
     *
     * @return the column's type, or {@code null} when the name is blank — not a name, so not this
     *     check's business: the builder rejects a blank one, and its message is the one both APIs
     *     should give
     */
    @Nullable
    private static LogicalType columnOf(String name, RowType rowType, ConfigOption<?> option) {
        if (StringUtils.isNullOrWhitespaceOnly(name)) {
            return null;
        }
        List<String> columnNames = rowType.getFieldNames();
        for (int i = 0; i < columnNames.size(); i++) {
            // equalsIgnoreCase rather than a lower-cased lookup: it is locale-independent by
            // definition, which is the hazard a toLowerCase() over column names has to think about.
            if (columnNames.get(i).equalsIgnoreCase(name)) {
                return rowType.getTypeAt(i);
            }
        }
        throw new ValidationException(
                String.format(
                        "Option '%s' names column '%s', which the table does not declare. BigQuery"
                                + " partitions and clusters on the table's own top-level columns:"
                                + " %s.",
                        option.key(), name, columnNames));
    }

    /**
     * Rejects a partitioning column BigQuery cannot partition on.
     *
     * <p>Two rules, both from the service's own documentation and both stated about column
     * <em>shape</em> rather than about a list that could grow: time-unit partitioning takes a
     * top-level, non-repeated {@code DATE}, {@code TIMESTAMP} or {@code DATETIME} column, and a
     * {@code DATE} column has no hourly granularity. In this connector's mapping those three
     * BigQuery types are exactly Flink's {@code DATE}, {@code TIMESTAMP_LTZ} and {@code TIMESTAMP}
     * — so the rule is checkable here, where the DDL says which is which, rather than at the first
     * record from inside a task. The emulator stores either mistake without complaint.
     */
    private static void checkPartitioningColumn(
            String name,
            @Nullable LogicalType column,
            @Nullable TableCreateOptions.TimePartitioningType granularity) {
        if (column == null) {
            return;
        }
        String key = BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_FIELD.key();
        LogicalTypeRoot root = column.getTypeRoot();
        if (root != LogicalTypeRoot.DATE
                && root != LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
                && root != LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' names column '%s' of type %s. BigQuery partitions on a"
                                    + " DATE, TIMESTAMP or DATETIME column, which here means a"
                                    + " DATE, TIMESTAMP_LTZ or TIMESTAMP column.",
                            key, name, column));
        }
        if (root == LogicalTypeRoot.DATE
                && granularity == TableCreateOptions.TimePartitioningType.HOUR) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' = 'hour' cannot partition on the DATE column '%s' that"
                                    + " '%s' names: a DATE column supports day, month and year"
                                    + " granularity only. Use a TIMESTAMP or TIMESTAMP_LTZ column,"
                                    + " or a coarser granularity.",
                            BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_TYPE.key(),
                            name,
                            key));
        }
    }

    /**
     * Rejects a clustering column BigQuery cannot cluster on.
     *
     * <p>Shape only — BigQuery clusters on top-level, non-repeated columns of a <em>scalar</em>
     * type, and an array, map, multiset or row column is none of those however it is marked. That
     * boundary is the shape of the feature and cannot move. Which <em>scalar</em> types are
     * clusterable is a separate question and deliberately not encoded: that list has grown before
     * (RANGE), and a stale copy here would refuse a table BigQuery would have created, which is a
     * worse failure than the late one it would prevent. So a {@code DOUBLE} or {@code TIME}
     * clustering column still reaches the service and is refused there.
     *
     * <p>One consequence worth naming rather than fixing: a {@code ROW} marked by {@code
     * sink.json-field-paths} derives a top-level, non-repeated {@code JSON} column, which is
     * excluded by the type list rather than by the shape rule — so it is refused here while the
     * same {@code JSON} column reached from a marked {@code STRING} is left to the service. Both
     * end in a table BigQuery will not create, and making this mapper read a second option family
     * to tell them apart would buy nothing.
     */
    private static void checkClusteringColumn(String name, @Nullable LogicalType column) {
        if (column == null) {
            return;
        }
        LogicalTypeRoot root = column.getTypeRoot();
        if (root == LogicalTypeRoot.ARRAY
                || root == LogicalTypeRoot.MULTISET
                || root == LogicalTypeRoot.MAP
                || root == LogicalTypeRoot.ROW) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' names column '%s' of type %s. BigQuery clusters on"
                                    + " top-level, non-repeated columns of a scalar type, which an"
                                    + " array, map, multiset or row column is not.",
                            BigQueryConnectorOptions.SINK_TABLE_CREATE_CLUSTERED_FIELDS.key(),
                            name,
                            column));
        }
    }
}
