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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.RowType;

import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for mapping {@code sink.table-create.*} and the DDL's families onto creation settings. */
class TableCreateOptionsMapperTest {

    private static final BigtableTableSchema TWO_FAMILIES =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf1",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q1", DataTypes.STRING()))),
                                            DataTypes.FIELD(
                                                    "cf2",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q2", DataTypes.BIGINT()))))
                                    .getLogicalType());

    private static Configuration configuration(String... keysAndValues) {
        Configuration config = new Configuration();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            config.setString(keysAndValues[i], keysAndValues[i + 1]);
        }
        return config;
    }

    @Test
    void aTableThatIsNotCreatedGetsNoCreationSettings() {
        assertThat(TableCreateOptionsMapper.map(new Configuration(), TWO_FAMILIES)).isNull();
        assertThat(
                        TableCreateOptionsMapper.map(
                                configuration("sink.create-disposition", "create-never"),
                                TWO_FAMILIES))
                .isNull();
    }

    @Test
    void theFamiliesComeFromTheDdlAndTakeTheSameRule() {
        TableCreateOptions options =
                TableCreateOptionsMapper.map(
                        configuration(
                                "sink.create-disposition", "create-if-needed",
                                "sink.table-create.gc-rule.max-versions", "3"),
                        TWO_FAMILIES);

        assertThat(options).isNotNull();
        assertThat(options.getColumnFamilies())
                .containsOnlyKeys("cf1", "cf2")
                .containsValues(GcRule.maxVersions(3));
    }

    @Test
    void anAgeRuleAloneIsAlsoEnough() {
        TableCreateOptions options =
                TableCreateOptionsMapper.map(
                        configuration(
                                "sink.create-disposition", "create-if-needed",
                                "sink.table-create.gc-rule.max-age", "7d"),
                        TWO_FAMILIES);

        assertThat(options).isNotNull();
        assertThat(options.getColumnFamilies()).containsValues(GcRule.maxAge(Duration.ofDays(7)));
    }

    @Test
    void bothRulesAreUnionedRatherThanIntersected() {
        // A union is "too old or too many versions", which is what a bounded history means; an
        // intersection would keep an ancient cell alive as long as it was also recent enough in
        // the version list.
        TableCreateOptions options =
                TableCreateOptionsMapper.map(
                        configuration(
                                "sink.create-disposition", "create-if-needed",
                                "sink.table-create.gc-rule.max-versions", "2",
                                "sink.table-create.gc-rule.max-age", "1h"),
                        TWO_FAMILIES);

        assertThat(options).isNotNull();
        assertThat(options.getColumnFamilies())
                .containsValues(
                        GcRule.union(GcRule.maxVersions(2), GcRule.maxAge(Duration.ofHours(1))));
    }

    @Test
    void creatingWithNoRuleIsRejected() {
        // The DataStream API allows a rule-less family; this layer does not, because an
        // at-least-once upsert sink writes another version of the same cells on every replay and
        // nothing would ever collect them.
        assertThatThrownBy(
                        () ->
                                TableCreateOptionsMapper.map(
                                        configuration(
                                                "sink.create-disposition", "create-if-needed"),
                                        TWO_FAMILIES))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("needs a garbage-collection rule")
                .hasMessageContaining("at-least-once");
    }

    @Test
    void aRuleKeyUnderADispositionThatCreatesNothingIsRejected() {
        // Both shapes: an explicit create-never, and the disposition left out entirely, where the
        // connector's own default decides and the builder's own check could never see the keys.
        for (String[] options :
                new String[][] {
                    {"sink.table-create.gc-rule.max-versions", "2"},
                    {
                        "sink.create-disposition", "create-never",
                        "sink.table-create.gc-rule.max-age", "1h"
                    }
                }) {
            assertThatThrownBy(
                            () ->
                                    TableCreateOptionsMapper.map(
                                            configuration(options), TWO_FAMILIES))
                    .isInstanceOf(ValidationException.class)
                    .hasMessageContaining("this table does not create any");
        }
    }

    @Test
    void theRejectedKeysAreQuotedTheWayTheFactoryQuotesItsOwn() {
        // The factory's two equivalent messages render a key list as 'a', 'b'; this one rendered a
        // List directly, so a SQL user met Java's brackets. Asserted on both halves so a
        // regression to either the brackets or an unquoted key fails here.
        assertThatThrownBy(
                        () ->
                                TableCreateOptionsMapper.map(
                                        configuration(
                                                "sink.table-create.gc-rule.max-versions", "2",
                                                "sink.table-create.gc-rule.max-age", "1h"),
                                        TWO_FAMILIES))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Options 'sink.table-create.gc-rule.max-versions',"
                                + " 'sink.table-create.gc-rule.max-age' describe")
                .hasMessageNotContaining("[");
    }

    @Test
    void namesTheOptionKeyWhenAGcRuleValueIsRejected() {
        assertThatThrownBy(
                        () ->
                                TableCreateOptionsMapper.map(
                                        configuration(
                                                "sink.create-disposition", "create-if-needed",
                                                "sink.table-create.gc-rule.max-versions", "0"),
                                        TWO_FAMILIES))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Option 'sink.table-create.gc-rule.max-versions' is invalid")
                .hasMessageContaining("maxVersions must be positive");
    }
}
