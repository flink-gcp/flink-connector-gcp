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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.bigquery.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableCreateOptionsMapper}. */
class TableCreateOptionsMapperTest {

    /**
     * A table whose columns cover every shape the checks care about: a partitionable {@code
     * TIMESTAMP_LTZ}, a partitionable {@code DATE}, two clusterable scalars, a scalar BigQuery does
     * <em>not</em> cluster on ({@code DOUBLE}), and a repeated one.
     */
    private static final RowType COLUMNS =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("name", DataTypes.STRING()),
                                    DataTypes.FIELD("event_ts", DataTypes.TIMESTAMP_LTZ(6)),
                                    DataTypes.FIELD("event_day", DataTypes.DATE()),
                                    DataTypes.FIELD("region", DataTypes.STRING()),
                                    DataTypes.FIELD("amount", DataTypes.DOUBLE()),
                                    DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING())))
                            .getLogicalType();

    /**
     * Every {@code TableCreateOptions.Builder} setter and the options that feed it.
     *
     * <p>The values are <em>lists</em>, unlike the sibling mapper tests' single options, because
     * {@code timePartitioning} is overloaded: one setter name carries both the granularity and the
     * column, and the pair of keys is what chooses between the two overloads. Written out rather
     * than derived, since no naming rule turns {@code clusteredFields} into {@code
     * clustered-fields} with the {@code sink.table-create.} prefix; the reflection test below is
     * what makes the table exhaustive.
     */
    private static final Map<String, List<ConfigOption<?>>> SETTER_TO_OPTIONS =
            new LinkedHashMap<>();

    static {
        SETTER_TO_OPTIONS.put(
                "timePartitioning",
                Arrays.asList(
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_TYPE,
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_FIELD));
        SETTER_TO_OPTIONS.put(
                "timePartitioningExpiration",
                Collections.singletonList(
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_EXPIRATION));
        SETTER_TO_OPTIONS.put(
                "clusteredFields",
                Collections.singletonList(
                        BigQueryConnectorOptions.SINK_TABLE_CREATE_CLUSTERED_FIELDS));
    }

    private static final String TYPE =
            BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_TYPE.key();
    private static final String FIELD =
            BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_FIELD.key();
    private static final String EXPIRATION =
            BigQueryConnectorOptions.SINK_TABLE_CREATE_TIME_PARTITIONING_EXPIRATION.key();
    private static final String CLUSTERED =
            BigQueryConnectorOptions.SINK_TABLE_CREATE_CLUSTERED_FIELDS.key();
    private static final String DISPOSITION =
            BigQueryConnectorOptions.SINK_CREATE_DISPOSITION.key();

    private static TableCreateOptions map(Map<String, String> options) {
        return TableCreateOptionsMapper.map(Configuration.fromMap(options), COLUMNS);
    }

    @Test
    void everyCreationKnobHasAnOption() {
        // Not filtered on arity: the overloaded timePartitioning is exactly the shape this guard
        // has to cover, and both its forms collapse to the one name.
        Set<String> setters =
                Arrays.stream(TableCreateOptions.Builder.class.getDeclaredMethods())
                        .filter(m -> Modifier.isPublic(m.getModifiers()))
                        .filter(m -> m.getReturnType() == TableCreateOptions.Builder.class)
                        .map(Method::getName)
                        .collect(Collectors.toSet());

        // Both directions: a new knob without an option, and an option whose knob was removed.
        assertThat(setters).containsExactlyInAnyOrderElementsOf(SETTER_TO_OPTIONS.keySet());
    }

    @Test
    void everyOptionOfTheFamilyFeedsAKnob() {
        // The other half of the guard above, and the one a new key would otherwise slip past: an
        // option declared under the sink.table-create.* prefix that no setter consumes. The
        // expected side is read out of BigQueryConnectorOptions rather than written here — a
        // literal list would only restate SETTER_TO_OPTIONS and could never disagree with it.
        Set<String> declared = OptionFamilies.declaredKeysUnder("sink.table-create.");
        // Guards the reflection itself: an empty set would make the assertion vacuous.
        assertThat(declared).isNotEmpty();

        Set<String> mapped =
                SETTER_TO_OPTIONS.values().stream()
                        .flatMap(List::stream)
                        .map(ConfigOption::key)
                        .collect(Collectors.toSet());

        assertThat(mapped).isEqualTo(declared);
    }

    @Test
    void noCreationOptionMeansNoObject() {
        assertThat(TableCreateOptionsMapper.map(new Configuration(), COLUMNS)).isNull();

        // Unrelated sink options do not conjure one either.
        Map<String, String> options = new HashMap<>();
        options.put(DISPOSITION, "create-if-needed");
        options.put(BigQueryConnectorOptions.SINK_LOCATION.key(), "US");
        assertThat(map(options)).isNull();
    }

    @Test
    void mapsEveryOptionOntoItsKnob() {
        Map<String, String> options = new HashMap<>();
        options.put(TYPE, "day");
        options.put(FIELD, "event_ts");
        options.put(EXPIRATION, "90 d");
        options.put(CLUSTERED, "region;name");

        TableCreateOptions mapped = map(options);

        assertThat(mapped.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.DAY);
        assertThat(mapped.getTimePartitioningField()).isEqualTo("event_ts");
        assertThat(mapped.getTimePartitioningExpirationMs())
                .isEqualTo(Duration.ofDays(90).toMillis());
        assertThat(mapped.getClusteredFields()).containsExactly("region", "name");
    }

    @Test
    void aGranularityWithoutAFieldPartitionsOnIngestionTime() {
        Map<String, String> options = new HashMap<>();
        options.put(TYPE, "month");

        TableCreateOptions mapped = map(options);

        assertThat(mapped.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.MONTH);
        // The whole reason the field is optional: ingestion time has no column to name.
        assertThat(mapped.getTimePartitioningField()).isNull();
    }

    @Test
    void anOptionLeftOutStaysUnsetRatherThanTakingAValue() {
        Map<String, String> options = new HashMap<>();
        options.put(CLUSTERED, "name");

        TableCreateOptions mapped = map(options);

        assertThat(mapped.getClusteredFields()).containsExactly("name");
        assertThat(mapped.getTimePartitioningType()).isNull();
        assertThat(mapped.getTimePartitioningField()).isNull();
        assertThat(mapped.getTimePartitioningExpirationMs()).isNull();
    }

    @Test
    void settingsAlongsideAnExplicitCreateNeverAreRejected() {
        // The disposition defaults to create-if-needed, so the settings alone are meaningful —
        // only saying "never create" while configuring what a created table looks like is the
        // contradiction.
        Map<String, String> options = new HashMap<>();
        options.put(DISPOSITION, "create-never");
        options.put(TYPE, "day");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(TYPE)
                .hasMessageContaining(DISPOSITION)
                .hasMessageContaining("create-never");
    }

    @Test
    void settingsWithoutADispositionRideTheCreateIfNeededDefault() {
        Map<String, String> options = new HashMap<>();
        options.put(TYPE, "day");

        assertThat(map(options)).isNotNull();

        options.put(DISPOSITION, "create-if-needed");
        assertThat(map(options)).isNotNull();
    }

    @Test
    void aPartitioningFieldWithoutAGranularityIsRejectedInOptionKeys() {
        // The builder cannot catch this one at all: its two timePartitioning overloads make a
        // field without a granularity unrepresentable, so there is no exception to inherit.
        Map<String, String> options = new HashMap<>();
        options.put(FIELD, "event_ts");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(FIELD)
                .hasMessageContaining(TYPE);
    }

    @Test
    void anExpirationWithoutAGranularityIsRejectedInOptionKeys() {
        // The builder rejects this too, but names timePartitioningExpiration.
        Map<String, String> options = new HashMap<>();
        options.put(EXPIRATION, "90 d");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(EXPIRATION)
                .hasMessageContaining(TYPE);
    }

    @Test
    void aPartitioningFieldOutsideTheTableIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put(TYPE, "day");
        options.put(FIELD, "created_at");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(FIELD)
                .hasMessageContaining("created_at")
                // The declared columns, so the message shows the spelling that was meant.
                .hasMessageContaining("event_ts");
    }

    @Test
    void aClusteringColumnOutsideTheTableIsRejected() {
        Map<String, String> options = new HashMap<>();
        options.put(CLUSTERED, "region;country");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(CLUSTERED)
                .hasMessageContaining("country");
    }

    @Test
    void aPartitioningColumnBigQueryCannotPartitionOnIsRejected() {
        // BigQuery partitions on DATE, TIMESTAMP or DATETIME — here DATE, TIMESTAMP_LTZ or
        // TIMESTAMP. A STRING column is refused at creation by the service and stored without
        // complaint by the emulator, so this is the only place it can be caught early.
        Map<String, String> options = new HashMap<>();
        options.put(TYPE, "day");
        options.put(FIELD, "region");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(FIELD)
                .hasMessageContaining("region")
                .hasMessageContaining("DATE, TIMESTAMP or DATETIME");
    }

    @Test
    void aDateColumnHasNoHourlyGranularity() {
        // A DATE column supports day, month and year only — the one granularity rule that depends
        // on the column, which is why it lives beside the column check rather than in the builder.
        Map<String, String> options = new HashMap<>();
        options.put(TYPE, "hour");
        options.put(FIELD, "event_day");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(TYPE)
                .hasMessageContaining("event_day")
                .hasMessageContaining("day, month and year granularity only");
    }

    @Test
    void aDateColumnPartitionsAtTheCoarserGranularities() {
        for (String granularity : new String[] {"day", "month", "year"}) {
            Map<String, String> options = new HashMap<>();
            options.put(TYPE, granularity);
            options.put(FIELD, "event_day");

            assertThat(map(options).getTimePartitioningField())
                    .as("granularity %s", granularity)
                    .isEqualTo("event_day");
        }
    }

    @Test
    void aRepeatedClusteringColumnIsRejected() {
        // "Cluster columns must be top-level, non-repeated columns" — an ARRAY derives a REPEATED
        // column, which BigQuery refuses to cluster on.
        Map<String, String> options = new HashMap<>();
        options.put(CLUSTERED, "tags");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(CLUSTERED)
                .hasMessageContaining("tags")
                .hasMessageContaining("top-level, non-repeated");
    }

    @Test
    void aScalarTypeBigQueryMayNotClusterOnIsLeftToTheService() {
        // Deliberately *not* rejected: the clusterable scalar type list has grown before (RANGE),
        // and a stale copy here would refuse a table BigQuery would have created. Structure is
        // checked, the type list is not — so this DOUBLE column passes and the service decides.
        // The column has to be one BigQuery really excludes, or the test would pass with the
        // policy reversed.
        Map<String, String> options = new HashMap<>();
        options.put(CLUSTERED, "amount");

        assertThat(map(options).getClusteredFields()).containsExactly("amount");
    }

    @Test
    void aTimestampColumnTakesEveryGranularityIncludingHour() {
        // The other side of the DATE rule, and the direction that catches a check written one
        // level too broadly: BigQuery documents hourly partitioning for TIMESTAMP and DATETIME.
        for (String granularity : new String[] {"hour", "day", "month", "year"}) {
            Map<String, String> options = new HashMap<>();
            options.put(TYPE, granularity);
            options.put(FIELD, "event_ts");

            assertThat(map(options).getTimePartitioningField())
                    .as("granularity %s", granularity)
                    .isEqualTo("event_ts");
        }
    }

    @Test
    void ingestionTimePartitioningTakesHourToo() {
        // The rule is about the column, so it must not reach a granularity that names none — and
        // a check placed one level up would reject exactly this.
        Map<String, String> options = new HashMap<>();
        options.put(TYPE, "hour");

        TableCreateOptions mapped = map(options);

        assertThat(mapped.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.HOUR);
        assertThat(mapped.getTimePartitioningField()).isNull();
    }

    @Test
    void aColumnSpelledInAnotherCaseIsAccepted() {
        // Case-insensitive on purpose: the check exists to catch a name that is not there at all,
        // and rejecting a case-only difference could refuse a table BigQuery would have created.
        Map<String, String> options = new HashMap<>();
        options.put(TYPE, "day");
        options.put(FIELD, "EVENT_TS");
        options.put(CLUSTERED, "Region");

        TableCreateOptions mapped = map(options);

        // The value reaches the builder unchanged rather than being normalised to the DDL's
        // spelling — BigQuery resolves it, and rewriting a user's value would be this layer
        // inventing behaviour the DataStream API does not have.
        assertThat(mapped.getTimePartitioningField()).isEqualTo("EVENT_TS");
        assertThat(mapped.getClusteredFields()).containsExactly("Region");
    }

    @Test
    void aBlankColumnNameIsTheBuildersToReport() {
        // The mapper's column lookup returns early for a blank name rather than reporting "the
        // table does not declare ''", which would be true but useless. Both shapes Flink's parsing
        // can produce are covered: an empty element inside a list, and an all-whitespace string.
        Map<String, String> blankInAList = new HashMap<>();
        // Built by concatenation: an adjacent pair of semicolons in a literal trips checkstyle's
        // "use one semicolon" rule, which does not read string literals.
        blankInAList.put(CLUSTERED, "region;" + ";name");
        assertThatThrownBy(() -> map(blankInAList))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("clustering columns must not be blank");

        Map<String, String> blankField = new HashMap<>();
        blankField.put(TYPE, "day");
        blankField.put(FIELD, "   ");
        assertThatThrownBy(() -> map(blankField))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("field must not be blank");
    }

    @Test
    void aValueTheBuilderRejectsKeepsTheBuildersOwnMessage() {
        // Value validation is not this mapper's: a fifth clustering column is the builder's
        // message, naming its own limit, so a SQL user and a DataStream user read the same thing.
        Map<String, String> options = new HashMap<>();
        options.put(CLUSTERED, "name;event_ts;region;name;event_ts");

        assertThatThrownBy(() -> map(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 4 clustering columns");
    }
}
