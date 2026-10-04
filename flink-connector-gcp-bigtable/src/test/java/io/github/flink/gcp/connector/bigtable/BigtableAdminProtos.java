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

package io.github.flink.gcp.connector.bigtable;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.GcRule;
import com.google.bigtable.admin.v2.Type;

import java.util.List;

/**
 * The admin protobufs the test harnesses create families with, so that no harness needs the
 * client's {@code @ObsoleteApi} model methods or the model types they take.
 *
 * <p>Each family spells what the model {@code CreateTableRequest}'s {@code addFamily} overload of
 * the same shape sent: a raw family is empty, and a typed or ruled family carries a rule, an empty
 * one standing for none.
 */
public final class BigtableAdminProtos {

    private static final Type BIG_ENDIAN_INT64 =
            Type.newBuilder()
                    .setInt64Type(
                            Type.Int64.newBuilder()
                                    .setEncoding(
                                            Type.Int64.Encoding.newBuilder()
                                                    .setBigEndianBytes(
                                                            Type.Int64.Encoding.BigEndianBytes
                                                                    .getDefaultInstance())))
                    .build();

    private BigtableAdminProtos() {}

    /** A raw family with no garbage-collection rule. */
    public static ColumnFamily rawFamily() {
        return ColumnFamily.getDefaultInstance();
    }

    /** A family with the given value type and no garbage-collection rule. */
    public static ColumnFamily typedFamily(Type type) {
        return ColumnFamily.newBuilder()
                .setGcRule(GcRule.getDefaultInstance())
                .setValueType(type)
                .build();
    }

    /** A raw family keeping at most the given number of versions. */
    public static ColumnFamily maxVersionsFamily(int versions) {
        return ColumnFamily.newBuilder().setGcRule(maxVersions(versions)).build();
    }

    /** A rule keeping at most the given number of versions. */
    public static GcRule maxVersions(int versions) {
        return GcRule.newBuilder().setMaxNumVersions(versions).build();
    }

    /** An Int64 encoded as big-endian bytes, the input of every aggregate created here. */
    public static Type bigEndianInt64() {
        return BIG_ENDIAN_INT64;
    }

    /** An Int64 sum aggregate over big-endian input. */
    public static Type int64Sum() {
        return aggregate(
                Type.Aggregate.newBuilder().setSum(Type.Aggregate.Sum.getDefaultInstance()));
    }

    /** An Int64 min aggregate over big-endian input. */
    public static Type int64Min() {
        return aggregate(
                Type.Aggregate.newBuilder().setMin(Type.Aggregate.Min.getDefaultInstance()));
    }

    /** An Int64 max aggregate over big-endian input. */
    public static Type int64Max() {
        return aggregate(
                Type.Aggregate.newBuilder().setMax(Type.Aggregate.Max.getDefaultInstance()));
    }

    /** An HLL++ unique-count aggregate over big-endian Int64 input. */
    public static Type int64Hll() {
        return aggregate(
                Type.Aggregate.newBuilder()
                        .setHllppUniqueCount(
                                Type.Aggregate.HyperLogLogPlusPlusUniqueCount
                                        .getDefaultInstance()));
    }

    /**
     * Refuses a listing that some locations did not answer, which cannot prove that anything is
     * absent. The client's model methods threw on it too.
     *
     * @param failedLocations the response's {@code failed_locations}
     * @param what what was being listed, for the message
     */
    public static void checkListedEverywhere(List<String> failedLocations, String what) {
        if (!failedLocations.isEmpty()) {
            throw new IllegalStateException(
                    "Bigtable could not list the " + what + " in " + failedLocations);
        }
    }

    private static Type aggregate(Type.Aggregate.Builder aggregate) {
        return Type.newBuilder().setAggregateType(aggregate.setInputType(BIG_ENDIAN_INT64)).build();
    }
}
