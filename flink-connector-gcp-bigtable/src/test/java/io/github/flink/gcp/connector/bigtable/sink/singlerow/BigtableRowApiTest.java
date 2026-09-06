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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the public surface of the single-row building blocks: what a user's program links
 * against must be connector-owned, so a client upgrade cannot change a public signature.
 */
class BigtableRowApiTest {

    @ParameterizedTest
    @CsvSource({"false, 0", "false, 1", "false, 3", "true, 0", "true, 1", "true, 3"})
    void acceptsImmutableCellsInOrder(boolean copyOf, int size) {
        BigtableRow.Cell first = cell("first", Collections.emptyList());
        BigtableRow.Cell second = cell("second", Collections.emptyList());
        List<BigtableRow.Cell> expected =
                new ArrayList<>(Arrays.asList(first, second, second).subList(0, size));
        List<BigtableRow.Cell> cells =
                copyOf ? List.copyOf(expected) : List.of(expected.toArray(new BigtableRow.Cell[0]));

        BigtableRow row = new BigtableRow(ByteString.EMPTY, cells);

        assertThat(row.getCells()).containsExactlyElementsOf(expected);
        assertThat(row)
                .isEqualTo(new BigtableRow(ByteString.EMPTY, expected))
                .hasSameHashCodeAs(new BigtableRow(ByteString.EMPTY, expected));
        assertThatThrownBy(() -> row.getCells().add(first))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest
    @CsvSource({"false, 0", "false, 1", "false, 3", "true, 0", "true, 1", "true, 3"})
    void acceptsImmutableLabelsInOrder(boolean copyOf, int size) {
        List<String> expected =
                new ArrayList<>(Arrays.asList("first", "second", "second").subList(0, size));
        List<String> labels =
                copyOf ? List.copyOf(expected) : List.of(expected.toArray(new String[0]));

        BigtableRow.Cell cell = cell("value", labels);

        assertThat(cell.getLabels()).containsExactlyElementsOf(expected);
        assertThat(cell)
                .isEqualTo(cell("value", expected))
                .hasSameHashCodeAs(cell("value", expected));
        assertThatThrownBy(() -> cell.getLabels().add("later"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsNullListsWithTheirParameterNames() {
        assertThatThrownBy(() -> new BigtableRow(ByteString.EMPTY, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("cells must not be null");
        assertThatThrownBy(() -> cell("value", null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("labels must not be null");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2})
    void rejectsNullElementsAtEveryPosition(int position) {
        BigtableRow.Cell cell = cell("value", Collections.emptyList());
        List<BigtableRow.Cell> cells = new ArrayList<>(Arrays.asList(cell, cell, cell));
        cells.set(position, null);
        assertThatThrownBy(() -> new BigtableRow(ByteString.EMPTY, cells))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("cells must not contain null");

        List<String> labels = new ArrayList<>(Arrays.asList("first", "second", "third"));
        labels.set(position, null);
        assertThatThrownBy(() -> cell("value", labels))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("labels must not contain null");
    }

    private static BigtableRow.Cell cell(String value, List<String> labels) {
        return new BigtableRow.Cell(
                "cf", ByteString.EMPTY, 1L, ByteString.copyFromUtf8(value), labels);
    }

    @Test
    void noPublicSignatureOfTheRowMentionsTheClientsModels() {
        // The client's Row and RowCell are @InternalExtensionOnly and change with the client; the
        // connector's mirror is what ADR-0148 promises to keep. ByteString is protobuf's and is
        // the row key's type everywhere else in this module, so it is allowed.
        List<Class<?>> mentioned = new ArrayList<>();
        for (Class<?> type : Arrays.asList(BigtableRow.class, BigtableRow.Cell.class)) {
            for (Method method : type.getMethods()) {
                if (Modifier.isPublic(method.getModifiers())) {
                    mentioned.add(method.getReturnType());
                    mentioned.addAll(Arrays.asList(method.getParameterTypes()));
                }
            }
            for (Constructor<?> constructor : type.getConstructors()) {
                mentioned.addAll(Arrays.asList(constructor.getParameterTypes()));
            }
        }

        assertThat(mentioned)
                .isNotEmpty()
                .noneMatch(type -> type.getName().startsWith("com.google.cloud.bigtable"));
    }

    @Test
    void theRowAndTheCellAreValueTypesWithoutAToString() throws Exception {
        BigtableRow.Cell cell =
                new BigtableRow.Cell(
                        "cf",
                        ByteString.copyFromUtf8("q"),
                        1L,
                        ByteString.copyFromUtf8("v"),
                        Collections.singletonList("l"));
        BigtableRow row = new BigtableRow(ByteString.copyFromUtf8("k"), Arrays.asList(cell));
        BigtableRow same =
                new BigtableRow(
                        ByteString.copyFromUtf8("k"),
                        Arrays.asList(
                                new BigtableRow.Cell(
                                        "cf",
                                        ByteString.copyFromUtf8("q"),
                                        1L,
                                        ByteString.copyFromUtf8("v"),
                                        Collections.singletonList("l"))));

        assertThat(row).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(row)
                .isNotEqualTo(
                        new BigtableRow(ByteString.copyFromUtf8("k"), Collections.emptyList()));
        // No toString: the key and the values are the row's own data, and a value type that
        // renders them is one log line away from putting them where they do not belong.
        assertThat(BigtableRow.class.getMethod("toString").getDeclaringClass())
                .isEqualTo(Object.class);
        assertThat(BigtableRow.Cell.class.getMethod("toString").getDeclaringClass())
                .isEqualTo(Object.class);
    }

    @Test
    void aFailedRequestRendersTheKeyLengthRatherThanTheKey() {
        FailedRequest failed =
                FailedRequest.of(
                        TableDestination.of("p", "i", "orders"),
                        RowOperation.READ_MODIFY_WRITE_ROW,
                        ByteString.copyFromUtf8("secret-key"),
                        "boom",
                        null);

        assertThat(failed.toString())
                .contains("p.i.orders", "READ_MODIFY_WRITE_ROW", "10 bytes", "boom")
                .doesNotContain("secret-key");
        assertThat(failed.getPayloadBytes()).isNull();
        assertThat(failed.getConnector()).isEqualTo("bigtable");
        assertThat(failed.describeDestination()).contains("orders");

        FailedRequest unbuilt =
                FailedRequest.of(TableDestination.of("p", "i", "orders"), null, null, "boom", null);
        assertThat(unbuilt.toString()).contains("operation=null", "rowKey=null");
    }

    @Test
    void theOperationsNameTheirRpcs() {
        assertThat(RowOperation.CHECK_AND_MUTATE_ROW.getRpcName()).isEqualTo("CheckAndMutateRow");
        assertThat(RowOperation.READ_MODIFY_WRITE_ROW.getRpcName()).isEqualTo("ReadModifyWriteRow");
    }
}
