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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FullCacheLookupKeysTest {

    private static final String REQUIREMENT = "A key must be a top-level scalar.";

    private static final DataType[] ACCEPTED = {
        DataTypes.BOOLEAN(),
        DataTypes.BIGINT(),
        DataTypes.DECIMAL(10, 2),
        DataTypes.CHAR(3),
        DataTypes.STRING(),
        DataTypes.BINARY(2),
        DataTypes.BYTES(),
        DataTypes.DATE(),
        DataTypes.TIMESTAMP_LTZ(6),
    };

    private static final DataType[] REFUSED = {
        DataTypes.FLOAT(),
        DataTypes.DOUBLE(),
        DataTypes.ARRAY(DataTypes.STRING()),
        DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
        DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING())),
        // Types no table produces: accepting one is the connector's decision to take.
        DataTypes.INT(),
        DataTypes.TIMESTAMP(6),
    };

    private static RowType row(DataType... types) {
        List<DataTypes.Field> fields = new ArrayList<>();
        for (int i = 0; i < types.length; i++) {
            fields.add(DataTypes.FIELD("c" + i, types[i]));
        }
        return (RowType) DataTypes.ROW(fields.toArray(new DataTypes.Field[0])).getLogicalType();
    }

    @Test
    void acceptsEveryTypeWhoseFlinkEqualityIsSqls() {
        RowType produced = row(ACCEPTED);

        for (int i = 0; i < ACCEPTED.length; i++) {
            int[][] keys = {{i}};
            assertThatNoException()
                    .as("%s", ACCEPTED[i])
                    .isThrownBy(() -> FullCacheLookupKeys.check(produced, keys, REQUIREMENT, null));
        }
        int[][] all = new int[ACCEPTED.length][];
        for (int i = 0; i < all.length; i++) {
            all[i] = new int[] {i};
        }
        assertThatNoException()
                .isThrownBy(() -> FullCacheLookupKeys.check(produced, all, REQUIREMENT, null));
    }

    @Test
    void refusesATypeWhoseFlinkEqualityIsNotSqlsAndNamesTheColumn() {
        RowType produced = row(REFUSED);

        for (int i = 0; i < REFUSED.length; i++) {
            int[][] keys = {{i}};
            assertThatThrownBy(() -> FullCacheLookupKeys.check(produced, keys, REQUIREMENT, null))
                    .as("%s", REFUSED[i])
                    .isInstanceOf(ValidationException.class)
                    .hasMessageStartingWith(REQUIREMENT)
                    .hasMessageContaining(
                            "Lookup column 'c" + i + "' has unsupported equality type")
                    .hasMessageContaining(REFUSED[i].getLogicalType().asSummaryString());
        }
    }

    @Test
    void anAcceptedKeyBesideARefusedOneDoesNotSaveIt() {
        RowType produced = row(DataTypes.BIGINT(), DataTypes.DOUBLE());

        assertThatThrownBy(
                        () ->
                                FullCacheLookupKeys.check(
                                        produced, new int[][] {{0}, {1}}, REQUIREMENT, null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Lookup column 'c1'");
    }

    @Test
    void refusesANestedKeyPath() {
        RowType produced = row(DataTypes.ROW(DataTypes.FIELD("a", DataTypes.STRING())));

        assertThatThrownBy(
                        () ->
                                FullCacheLookupKeys.check(
                                        produced, new int[][] {{0, 0}}, REQUIREMENT, null))
                .isInstanceOf(ValidationException.class)
                .hasMessage(REQUIREMENT + " A nested key path is not supported.");
    }

    @Test
    void refusesNoKeyWithTheRequirementAndTheConnectorsDetail() {
        RowType produced = row(DataTypes.BIGINT());

        assertThatThrownBy(
                        () -> FullCacheLookupKeys.check(produced, new int[0][], REQUIREMENT, null))
                .isInstanceOf(ValidationException.class)
                .hasMessage(REQUIREMENT);
        assertThatThrownBy(
                        () ->
                                FullCacheLookupKeys.check(
                                        produced, new int[0][], REQUIREMENT, "A cast is no key."))
                .isInstanceOf(ValidationException.class)
                .hasMessage(REQUIREMENT + " A cast is no key.");
    }
}
