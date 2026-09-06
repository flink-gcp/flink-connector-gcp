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

package io.github.flink.gcp.connector.bigtable.sink.tables;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.bigtable.admin.v2.Type;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.ColumnFamilyType;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnFamilyTypesTest {
    private static final TableDestination TABLE = TableDestination.of("p", "i", "t");

    @ParameterizedTest
    @EnumSource(ColumnFamilyType.class)
    void createsAndAddsTheExactFamilyTypeAndGcRule(ColumnFamilyType type) {
        var options =
                TableCreateOptions.builder()
                        .columnFamily("cf", type, GcRule.maxVersions(2))
                        .build();
        var expected =
                com.google.bigtable.admin.v2.ColumnFamily.newBuilder()
                        .setGcRule(
                                com.google.bigtable.admin.v2.GcRule.newBuilder()
                                        .setMaxNumVersions(2));
        if (type != ColumnFamilyType.RAW) {
            expected.setValueType(proto(type));
        }
        assertThat(
                        BigtableTableAdmin.toCreateTableRequest(TABLE, options)
                                .toProto("p", "i")
                                .getTable()
                                .getColumnFamiliesOrThrow("cf"))
                .isEqualTo(expected.build());
        assertThat(
                        BigtableTableAdmin.toModifyColumnFamiliesRequest(
                                        TABLE,
                                        options.getColumnFamilies(),
                                        options.getColumnFamilyTypes())
                                .toProto("p", "i")
                                .getModifications(0)
                                .getCreate())
                .isEqualTo(expected.build());
    }

    @Test
    void ignoresOutputStateAndUnrelatedFutureTypesButChecksInputEncoding() {
        Type sum = proto(ColumnFamilyType.INT64_SUM);
        Type live =
                sum.toBuilder()
                        .setAggregateType(
                                sum.getAggregateType().toBuilder()
                                        .setStateType(
                                                Type.newBuilder()
                                                        .setBytesType(
                                                                Type.Bytes.getDefaultInstance())))
                        .build();
        ColumnFamilyTypes.check(
                TABLE,
                Map.of("cf", ColumnFamilyType.INT64_SUM),
                Map.of(
                        "cf",
                        live,
                        "unrelated",
                        Type.newBuilder().setStringType(Type.String.getDefaultInstance()).build()),
                false);
        Type unknownInput =
                sum.toBuilder()
                        .setAggregateType(
                                sum.getAggregateType().toBuilder()
                                        .setInputType(
                                                Type.newBuilder()
                                                        .setBytesType(
                                                                Type.Bytes.getDefaultInstance())))
                        .build();
        assertThatThrownBy(
                        () ->
                                ColumnFamilyTypes.check(
                                        TABLE,
                                        Map.of("cf", ColumnFamilyType.INT64_SUM),
                                        Map.of("cf", unknownInput),
                                        false))
                .hasMessageContaining("cf")
                .hasMessageContaining("bytes_type")
                .hasMessageContaining("expected int64-sum");
    }

    @Test
    void normalizesOmittedIntegerEncodingWithoutDiscardingUnknownInputFields() {
        Type sum = proto(ColumnFamilyType.INT64_SUM);
        Type omitted =
                sum.toBuilder()
                        .setAggregateType(
                                sum.getAggregateType().toBuilder()
                                        .setInputType(
                                                Type.newBuilder()
                                                        .setInt64Type(
                                                                Type.Int64.getDefaultInstance())))
                        .build();
        ColumnFamilyTypes.check(
                TABLE, Map.of("cf", ColumnFamilyType.INT64_SUM), Map.of("cf", omitted), false);
        Type future =
                omitted.toBuilder()
                        .setAggregateType(
                                omitted.getAggregateType().toBuilder()
                                        .setInputType(
                                                omitted
                                                        .getAggregateType()
                                                        .getInputType()
                                                        .toBuilder()
                                                        .setInt64Type(
                                                                Type.Int64.newBuilder()
                                                                        .setEncoding(
                                                                                Type.Int64.Encoding
                                                                                        .newBuilder()
                                                                                        .setUnknownFields(
                                                                                                com
                                                                                                        .google
                                                                                                        .protobuf
                                                                                                        .UnknownFieldSet
                                                                                                        .newBuilder()
                                                                                                        .addField(
                                                                                                                99,
                                                                                                                com
                                                                                                                        .google
                                                                                                                        .protobuf
                                                                                                                        .UnknownFieldSet
                                                                                                                        .Field
                                                                                                                        .newBuilder()
                                                                                                                        .addVarint(
                                                                                                                                1)
                                                                                                                        .build())
                                                                                                        .build())))))
                        .build();
        assertThatThrownBy(
                        () ->
                                ColumnFamilyTypes.check(
                                        TABLE,
                                        Map.of("cf", ColumnFamilyType.INT64_SUM),
                                        Map.of("cf", future),
                                        false))
                .hasMessageContaining("expected int64-sum");
    }

    @Test
    void reportsBothTypesAndRejectsMissingRequiredFamilies() {
        assertThatThrownBy(
                        () ->
                                ColumnFamilyTypes.check(
                                        TABLE,
                                        Map.of("cf", ColumnFamilyType.INT64_SUM),
                                        Map.of("cf", Type.getDefaultInstance()),
                                        false))
                .hasMessageContaining("column family 'cf' has value type raw; expected int64-sum");
        assertThatThrownBy(
                        () ->
                                ColumnFamilyTypes.check(
                                        TABLE,
                                        Map.of("cf", ColumnFamilyType.RAW),
                                        Map.of("cf", proto(ColumnFamilyType.INT64_HLL)),
                                        false))
                .hasMessageContaining("int64-hll; expected raw");
        assertThatThrownBy(
                        () ->
                                ColumnFamilyTypes.check(
                                        TABLE,
                                        Map.of("cf", ColumnFamilyType.INT64_SUM),
                                        Map.of(),
                                        false))
                .hasMessageContaining("<missing>; expected int64-sum");
        ColumnFamilyTypes.check(TABLE, Map.of("cf", ColumnFamilyType.INT64_SUM), Map.of(), true);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void rawOnlyRepairAcceptsAnExistingAggregate(boolean missingRawFamily) {
        var options =
                TableCreateOptions.builder().columnFamily("totals").columnFamily("plain").build();
        List<com.google.bigtable.admin.v2.ModifyColumnFamiliesRequest> additions =
                new ArrayList<>();

        var result =
                BigtableTableAdmin.ensureWith(
                        TABLE,
                        options,
                        ignored -> {
                            throw exists();
                        },
                        ignored ->
                                missingRawFamily
                                        ? Map.of("totals", proto(ColumnFamilyType.INT64_SUM))
                                        : Map.of(
                                                "totals", proto(ColumnFamilyType.INT64_SUM),
                                                "plain", Type.getDefaultInstance()),
                        request -> additions.add(request.toProto("p", "i")));

        assertThat(result.columnFamiliesAdded()).isEqualTo(missingRawFamily ? 1 : 0);
        assertThat(result.existingColumnFamilies()).containsExactlyInAnyOrder("totals", "plain");
        if (missingRawFamily) {
            assertThat(additions)
                    .containsExactly(
                            com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest
                                    .of("t")
                                    .addFamily("plain")
                                    .toProto("p", "i"));
        } else {
            assertThat(additions).isEmpty();
        }
    }

    @Test
    void aggregateDeclarationsAlsoValidateDeclaredRawFamilies() {
        var options =
                TableCreateOptions.builder()
                        .columnFamily("totals", ColumnFamilyType.INT64_SUM, null)
                        .columnFamily("plain")
                        .build();
        assertThatThrownBy(
                        () ->
                                BigtableTableAdmin.ensureWith(
                                        TABLE,
                                        options,
                                        ignored -> {
                                            throw exists();
                                        },
                                        ignored ->
                                                Map.of("plain", proto(ColumnFamilyType.INT64_MIN)),
                                        ignored -> {
                                            throw new AssertionError("must reject before adding");
                                        }))
                .isInstanceOf(ColumnFamilyTypes.Mismatch.class)
                .hasMessageContaining(
                        "column family 'plain' has value type int64-min; expected raw");
    }

    @ParameterizedTest
    @EnumSource(value = ColumnFamilyType.class, names = "RAW", mode = EnumSource.Mode.EXCLUDE)
    @SuppressWarnings("deprecation")
    void ignoresDeprecatedIntegerBytesTypeButPreservesUnknownEncodingFields(ColumnFamilyType type) {
        Type expected = proto(type);
        var aggregate = expected.getAggregateType().toBuilder();
        var input = aggregate.getInputType().toBuilder();
        var integer = input.getInt64Type().toBuilder();
        var encoding = integer.getEncoding().toBuilder();
        var bigEndian =
                encoding.getBigEndianBytes().toBuilder()
                        .setBytesType(Type.Bytes.getDefaultInstance());
        encoding.setBigEndianBytes(bigEndian);
        integer.setEncoding(encoding);
        input.setInt64Type(integer);
        aggregate.setInputType(input);
        Type live = expected.toBuilder().setAggregateType(aggregate).build();
        ColumnFamilyTypes.check(TABLE, Map.of("cf", type), Map.of("cf", live), false);

        bigEndian.setUnknownFields(
                com.google.protobuf.UnknownFieldSet.newBuilder()
                        .addField(
                                99,
                                com.google.protobuf.UnknownFieldSet.Field.newBuilder()
                                        .addVarint(1)
                                        .build())
                        .build());
        encoding.setBigEndianBytes(bigEndian);
        integer.setEncoding(encoding);
        input.setInt64Type(integer);
        aggregate.setInputType(input);
        Type future = expected.toBuilder().setAggregateType(aggregate).build();
        assertThatThrownBy(
                        () ->
                                ColumnFamilyTypes.check(
                                        TABLE, Map.of("cf", type), Map.of("cf", future), false))
                .isInstanceOf(ColumnFamilyTypes.Mismatch.class)
                .hasMessageContaining("expected " + type);
    }

    @Test
    void rechecksTheTypeWhenAConcurrentCreatorWins() {
        AtomicInteger reads = new AtomicInteger();
        List<Object> additions = new ArrayList<>();
        var options =
                TableCreateOptions.builder()
                        .columnFamily("cf", ColumnFamilyType.INT64_SUM, null)
                        .build();
        assertThatThrownBy(
                        () ->
                                BigtableTableAdmin.ensureWith(
                                        TABLE,
                                        options,
                                        ignored -> {
                                            throw exists();
                                        },
                                        ignored ->
                                                reads.getAndIncrement() == 0
                                                        ? Map.of()
                                                        : Map.of(
                                                                "cf",
                                                                proto(ColumnFamilyType.INT64_MIN)),
                                        request -> {
                                            additions.add(request);
                                            throw exists();
                                        }))
                .isInstanceOf(ColumnFamilyTypes.Mismatch.class)
                .hasMessageContaining("int64-min; expected int64-sum");
        assertThat(reads.get()).isEqualTo(2);
        assertThat(additions).hasSize(1);
    }

    private static AlreadyExistsException exists() {
        return new AlreadyExistsException(
                "exists", null, GrpcStatusCode.of(Status.Code.ALREADY_EXISTS), false);
    }

    private static Type proto(ColumnFamilyType type) {
        Type.Aggregate.Builder aggregate =
                Type.Aggregate.newBuilder()
                        .setInputType(
                                Type.newBuilder()
                                        .setInt64Type(
                                                Type.Int64.newBuilder()
                                                        .setEncoding(
                                                                Type.Int64.Encoding.newBuilder()
                                                                        .setBigEndianBytes(
                                                                                Type.Int64.Encoding
                                                                                        .BigEndianBytes
                                                                                        .getDefaultInstance()))));
        switch (type) {
            case RAW:
                return Type.getDefaultInstance();
            case INT64_SUM:
                aggregate.setSum(Type.Aggregate.Sum.getDefaultInstance());
                break;
            case INT64_MIN:
                aggregate.setMin(Type.Aggregate.Min.getDefaultInstance());
                break;
            case INT64_MAX:
                aggregate.setMax(Type.Aggregate.Max.getDefaultInstance());
                break;
            case INT64_HLL:
                aggregate.setHllppUniqueCount(
                        Type.Aggregate.HyperLogLogPlusPlusUniqueCount.getDefaultInstance());
                break;
            default:
                throw new AssertionError(type);
        }
        return Type.newBuilder().setAggregateType(aggregate).build();
    }
}
