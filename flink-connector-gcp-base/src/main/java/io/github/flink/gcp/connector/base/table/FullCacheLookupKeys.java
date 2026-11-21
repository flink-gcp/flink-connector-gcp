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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/**
 * The planning check of the lookup keys that index a scan-loaded {@code FULL} cache.
 *
 * <p>Flink's full cache keeps every loaded row under the projection of the planner's lookup keys
 * and finds a probe's rows by {@code GenericRowData.equals}, so a key must be a top-level field of
 * the produced row and of a type whose Flink value equality agrees with SQL {@code =}. The Spanner,
 * Firestore and Datastore lookup sources share this check (#1847), each with its own requirement
 * message; whether a connector accepts {@code FULL} at all, and how it loads the rows, stays with
 * the connector.
 */
@Internal
public final class FullCacheLookupKeys {

    private FullCacheLookupKeys() {}

    /**
     * Checks the planner's lookup keys of a {@code FULL} cache.
     *
     * @param producedType the row the load produces, which the keys index into
     * @param keys the planner's lookup keys, each a path of field indexes
     * @param requirement the connector's sentence stating what a key must be; every refusal starts
     *     with it
     * @param noKeyDetail a sentence appended when the planner passed no key at all, or {@code null}
     *     to give the requirement alone
     * @throws ValidationException if there is no key, a key is a nested path, or a key's type is
     *     one whose Flink value equality is not SQL's
     */
    public static void check(
            RowType producedType, int[][] keys, String requirement, @Nullable String noKeyDetail) {
        Preconditions.checkNotNull(producedType, "producedType must not be null");
        Preconditions.checkNotNull(keys, "keys must not be null");
        Preconditions.checkNotNull(requirement, "requirement must not be null");
        if (keys.length == 0) {
            throw new ValidationException(
                    noKeyDetail == null ? requirement : requirement + " " + noKeyDetail);
        }
        for (int[] key : keys) {
            if (key.length != 1) {
                throw new ValidationException(requirement + " A nested key path is not supported.");
            }
            LogicalType type = producedType.getTypeAt(key[0]);
            if (!hasSqlEquality(type)) {
                throw new ValidationException(
                        requirement
                                + " Lookup column '"
                                + producedType.getFieldNames().get(key[0])
                                + "' has unsupported equality type "
                                + type.asSummaryString()
                                + ".");
            }
        }
    }

    /**
     * Whether a type's Flink values match in the full cache's index exactly when SQL equality
     * holds, among the types the Spanner, Firestore and Datastore tables produce. {@code FLOAT} and
     * {@code DOUBLE} do not: their boxed {@code equals} tells {@code -0.0} from {@code 0.0} and
     * matches {@code NaN} to itself. {@code ARRAY}, {@code MAP} and {@code ROW} do not either,
     * because their binary and generic forms are not equal to each other. A type no table produces
     * is refused too, so that accepting one is a decision taken with the connector that adds it.
     *
     * @param type a key column's type
     * @return whether the type may key the cache
     */
    static boolean hasSqlEquality(LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
            case BIGINT:
            case DECIMAL:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DATE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }
}
