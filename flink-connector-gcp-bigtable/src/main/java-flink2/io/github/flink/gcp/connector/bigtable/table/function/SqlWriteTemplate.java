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
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.options.ResourceNames;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalMutation;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalRequest;
import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalRequests;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRequest;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRequests;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRule;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRequestOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableRow;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.writer.RowRequest;
import io.github.flink.gcp.connector.bigtable.table.CellValueCodec;
import io.github.flink.gcp.connector.bigtable.table.OptionSetters;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Serializable plan state; no runtime client, credentials or session configuration is retained. */
@Internal
final class SqlWriteTemplate implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int MAX_OPERATIONS = 100_000;
    final String name;
    final TableDestination destination;
    final String appProfileId;
    final String keyFile;
    final EmulatorEndpoint emulator;
    final BigtableRequestOptions options;
    private final CellValueCodec.FieldEncoder keyEncoder;
    private final SqlFilterTemplate predicate;
    private final List<SqlMutationTemplate> then;
    private final List<SqlMutationTemplate> otherwise;
    private final List<SqlRuleTemplate> rules;
    private final Map<SqlRuleTemplate.Cell, Boolean> finalIncrements;

    private SqlWriteTemplate(
            String name,
            TableDestination destination,
            String appProfileId,
            String keyFile,
            EmulatorEndpoint emulator,
            BigtableRequestOptions options,
            CellValueCodec.FieldEncoder keyEncoder,
            SqlFilterTemplate predicate,
            List<SqlMutationTemplate> then,
            List<SqlMutationTemplate> otherwise,
            List<SqlRuleTemplate> rules) {
        this.name = name;
        this.destination = destination;
        this.appProfileId = appProfileId;
        this.keyFile = keyFile;
        this.emulator = emulator;
        this.options = options;
        this.keyEncoder = keyEncoder;
        this.predicate = predicate;
        this.then = List.copyOf(then);
        this.otherwise = List.copyOf(otherwise);
        this.rules = List.copyOf(rules);
        Map<SqlRuleTemplate.Cell, Boolean> finalOperations = new HashMap<>();
        rules.forEach(rule -> finalOperations.put(rule.cell, rule.increment));
        finalIncrements = Map.copyOf(finalOperations);
    }

    static SqlWriteTemplate compile(
            String name, boolean conditional, ReadableConfig config, List<DataType> argumentTypes) {
        if (name.isBlank() || !name.equals(name.strip()) || name.contains(".")) {
            throw new ValidationException(
                    "Bigtable function settings name must be nonblank, without dots or edge whitespace.");
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_ATTEMPTS) != 1) {
            throw new ValidationException(
                    "Bigtable async SQL writes require SET 'table.exec.async-scalar.max-attempts' = '1'; an automatic retry can repeat a committed write.");
        }
        if (config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_CONCURRENT_OPERATIONS)
                <= 0) {
            throw new ValidationException(
                    "Option 'table.exec.async-scalar.max-concurrent-operations' must be positive.");
        }
        Map<String, String> selected = new HashMap<>();
        config.getOptional(BigtableSqlFunctionOptions.FUNCTIONS)
                .orElse(Map.of())
                .forEach(
                        (key, value) -> {
                            if (key.startsWith(name + ".")) {
                                selected.put(key.substring(name.length() + 1), value);
                            }
                        });
        SqlSettingMap settings = new SqlSettingMap("bigtable.functions." + name + ".", selected);
        String project = component(settings, "project");
        String instance = component(settings, "instance");
        String table = component(settings, "table");
        String appProfile = settings.optional("app-profile-id");
        if (appProfile != null && appProfile.isBlank()) {
            throw settings.error("app-profile-id", "must not be blank");
        }
        String keyFile = settings.optional("service-account-key-file");
        String endpoint = settings.optional("emulator-endpoint");
        if (keyFile != null && keyFile.isBlank()) {
            throw settings.error("service-account-key-file", "must not be blank");
        }
        if (keyFile != null && endpoint != null) {
            throw settings.error(
                    "service-account-key-file", "cannot be combined with emulator-endpoint");
        }
        EmulatorEndpoint emulator =
                endpoint == null
                        ? null
                        : EmulatorEndpoint.parse(endpoint, settings.key("emulator-endpoint"));
        BigtableRequestOptions.Builder builder =
                BigtableRequestOptions.builder().maxActiveInstances(1);
        String timeout = settings.optional("request-timeout");
        if (timeout != null) {
            Duration duration =
                    OptionSetters.convert(
                            settings.key("request-timeout"),
                            timeout,
                            value ->
                                    Configuration.fromMap(Map.of("request-timeout", value))
                                            .get(
                                                    ConfigOptions.key("request-timeout")
                                                            .durationType()
                                                            .noDefaultValue()));
            OptionSetters.accept(
                    settings.key("request-timeout"), duration, builder::requestTimeout);
        }
        BigtableRequestOptions options = builder.build();
        try {
            long outerMillis =
                    config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_TIMEOUT).toMillis();
            if (outerMillis <= options.getRequestTimeout().toMillis()) {
                throw settings.error(
                        "request-timeout",
                        "must be shorter than table.exec.async-scalar.timeout after millisecond conversion");
            }
        } catch (ArithmeticException e) {
            throw new ValidationException(
                    "Option 'table.exec.async-scalar.timeout' must fit in milliseconds.");
        }
        SqlFilterTemplate predicate = null;
        List<SqlMutationTemplate> then = new ArrayList<>();
        List<SqlMutationTemplate> otherwise = new ArrayList<>();
        List<SqlRuleTemplate> rules = new ArrayList<>();
        if (conditional) {
            predicate = SqlFilterTemplate.parse(settings.section("predicate"), argumentTypes);
            for (SqlSettingMap mutation : bounded(settings, "then")) {
                then.add(SqlMutationTemplate.parse(mutation, argumentTypes));
            }
            for (SqlSettingMap mutation : bounded(settings, "otherwise")) {
                otherwise.add(SqlMutationTemplate.parse(mutation, argumentTypes));
            }
            if (then.isEmpty() && otherwise.isEmpty()) {
                throw settings.error("then", "and otherwise cannot both be empty");
            }
        } else {
            for (SqlSettingMap rule : bounded(settings, "rules")) {
                rules.add(SqlRuleTemplate.parse(rule, argumentTypes));
            }
            if (rules.isEmpty()) {
                throw settings.error("rules", "requires at least one rule");
            }
        }
        settings.finish();
        CellValueCodec.checkSupported("row key", argumentTypes.get(1).getLogicalType());
        return new SqlWriteTemplate(
                name,
                TableDestination.of(project, instance, table),
                appProfile,
                keyFile,
                emulator,
                options,
                CellValueCodec.encoder(argumentTypes.get(1).getLogicalType()),
                predicate,
                then,
                otherwise,
                rules);
    }

    private static String component(SqlSettingMap settings, String property) {
        return OptionSetters.convert(
                settings.key(property),
                settings.required(property),
                value -> ResourceNames.checkComponent(value, property));
    }

    private static List<SqlSettingMap> bounded(SqlSettingMap settings, String property) {
        List<SqlSettingMap> values = settings.numbered(property);
        if (values.size() > MAX_OPERATIONS) {
            throw settings.error(property, "exceeds the 100,000-operation limit");
        }
        return values;
    }

    RowRequest<?> request(RowData input) {
        if (input.isNullAt(0)) {
            throw new ValidationException("Bigtable SQL write row key must not be NULL.");
        }
        ByteString key = ByteString.copyFrom(keyEncoder.encode(input, 0));
        if (key.isEmpty()) {
            throw new ValidationException("Bigtable SQL write row key must not be empty.");
        }
        if (predicate != null) {
            List<ConditionalMutation> whenTrue = new ArrayList<>();
            List<ConditionalMutation> whenFalse = new ArrayList<>();
            then.forEach(mutation -> whenTrue.add(mutation.instantiate(input)));
            otherwise.forEach(mutation -> whenFalse.add(mutation.instantiate(input)));
            return ConditionalRequests.adapt(
                    ConditionalRequest.of(key, predicate.instantiate(input), whenTrue, whenFalse));
        }
        List<ReadModifyWriteRule> requestRules = new ArrayList<>();
        rules.forEach(rule -> requestRules.add(rule.instantiate(input)));
        return ReadModifyWriteRequests.adapt(ReadModifyWriteRequest.of(key, requestRules));
    }

    Object result(Object response) {
        if (predicate != null) {
            return (Boolean) response;
        }
        BigtableRow row = (BigtableRow) response;
        Row[] cells = new Row[row.getCells().size()];
        for (int i = 0; i < cells.length; i++) {
            BigtableRow.Cell cell = row.getCells().get(i);
            byte[] value = cell.getValue().toByteArray();
            Long integer = null;
            if (Boolean.TRUE.equals(
                    finalIncrements.get(
                            new SqlRuleTemplate.Cell(cell.getFamily(), cell.getQualifier())))) {
                if (value.length != Long.BYTES) {
                    throw new IllegalStateException(
                            "Bigtable returned an increment cell without an eight-byte value.");
                }
                integer = ByteBuffer.wrap(value).getLong();
            }
            cells[i] =
                    Row.of(
                            cell.getFamily(),
                            cell.getQualifier().toByteArray(),
                            value,
                            cell.getTimestampMicros(),
                            integer);
        }
        return Row.of(row.getKey().toByteArray(), cells);
    }
}
