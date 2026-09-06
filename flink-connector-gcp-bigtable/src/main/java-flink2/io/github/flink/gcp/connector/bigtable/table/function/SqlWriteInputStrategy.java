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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;

import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Keeps each operand's SQL type and bridges its value directly to the existing cell codec. */
@Internal
final class SqlWriteInputStrategy implements InputTypeStrategy {
    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.from(2);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(CallContext context, boolean throwOnFailure) {
        List<DataType> arguments = context.getArgumentDataTypes();
        if (arguments.size() < 2
                || !context.isArgumentLiteral(0)
                || context.getArgumentValue(0, String.class).isEmpty()) {
            return context.fail(
                    throwOnFailure,
                    "A Bigtable SQL write requires a literal settings name and a row key.");
        }
        List<DataType> types = new ArrayList<>();
        types.add(DataTypes.STRING().notNull());
        try {
            for (int i = 1; i < arguments.size(); i++) {
                DataType argument = arguments.get(i);
                if (argument.getLogicalType().isNullable()) {
                    return context.fail(
                            throwOnFailure,
                            "Bigtable SQL %s must have a NOT NULL type; nullable expressions are not accepted.",
                            i == 1 ? "row key" : "value argument " + (i - 2));
                }
                CellValueCodec.checkSupported(
                        i == 1 ? "row key" : "value argument " + (i - 2),
                        argument.getLogicalType());
                types.add(bridge(argument));
            }
        } catch (IllegalArgumentException | ValidationException e) {
            return context.fail(throwOnFailure, "%s", e.getMessage());
        }
        return Optional.of(types);
    }

    private static DataType bridge(DataType type) {
        switch (type.getLogicalType().getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return type.bridgedTo(StringData.class);
            case DECIMAL:
                return type.bridgedTo(DecimalData.class);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return type.bridgedTo(TimestampData.class);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                return type.bridgedTo(Integer.class);
            case INTERVAL_DAY_TIME:
                return type.bridgedTo(Long.class);
            default:
                return type;
        }
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return List.of(
                Signature.of(
                        Signature.Argument.of("settings", "STRING LITERAL"),
                        Signature.Argument.of("row_key", "SCALAR NOT NULL"),
                        Signature.Argument.ofVarying("values", "SCALAR NOT NULL")));
    }
}
