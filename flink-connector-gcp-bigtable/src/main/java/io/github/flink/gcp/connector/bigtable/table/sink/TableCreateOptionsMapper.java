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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigtable.sink.CreateDisposition;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableConnectorOptions;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Builds {@link TableCreateOptions} from the table options and the DDL's column families.
 *
 * <p><b>The families come from the DDL, never from a key.</b> A {@code ROW<...>} column already
 * says a family exists and what is written into it, so naming the same families again in the {@code
 * WITH} clause would only create a way for the two lists to disagree.
 *
 * <p><b>The garbage-collection rule does not.</b> {@link GcRule} is a tree — unions and
 * intersections of version and age limits, to any depth — and a flat {@code WITH} namespace cannot
 * carry one. So the DDL surface is the contraction: at most a version limit and an age limit,
 * unioned when both are given, and the same rule for every family. A family that needs anything
 * else is created out of band, which is what {@code create-never} is for; nothing about the
 * DataStream API's {@code GcRule} changes.
 *
 * <p><b>At least one of the two rule keys is required</b>, which the DataStream API does not
 * require. A column family created with no rule keeps every version of every cell forever, and this
 * sink is at-least-once and upsert-shaped: each replay of a row writes another version of the same
 * cells, so a rule-less family created from a DDL grows without bound and nothing reports it. A
 * user who genuinely wants unbounded versions creates the family themselves.
 */
@Internal
public final class TableCreateOptionsMapper {

    private static final List<ConfigOption<?>> FAMILY =
            Arrays.asList(
                    BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_VERSIONS,
                    BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_AGE);

    private TableCreateOptionsMapper() {}

    /**
     * Maps the table options and the DDL's families onto creation settings.
     *
     * @param config the table options
     * @param schema the parsed DDL model, which is where the families come from
     * @return the creation settings, or {@code null} when the table is not created by the sink
     */
    @Nullable
    public static TableCreateOptions map(ReadableConfig config, BigtableTableSchema schema) {
        if (config.getOptional(BigtableConnectorOptions.SINK_CREATE_DISPOSITION).orElse(null)
                != CreateDisposition.CREATE_IF_NEEDED) {
            rejectUnusedCreationKeys(config);
            return null;
        }
        GcRule rule = gcRule(config);
        TableCreateOptions.Builder builder = TableCreateOptions.builder();
        for (BigtableTableSchema.Family family : schema.getFamilies()) {
            builder.columnFamily(family.getName(), rule);
        }
        return builder.build();
    }

    /**
     * Rejects {@code sink.table-create.*} keys under a disposition that creates nothing.
     *
     * <p>The builder rejects the pair too — {@code tableCreateOptions} without {@code
     * CREATE_IF_NEEDED} — but its message names those setters, which appear nowhere in a {@code
     * WITH} clause. This says the same thing in keys, and it also covers the case the builder
     * cannot see: the disposition left out entirely, where the connector's own default decides.
     */
    private static void rejectUnusedCreationKeys(ReadableConfig config) {
        List<String> present = new ArrayList<>(FAMILY.size());
        for (ConfigOption<?> option : FAMILY) {
            if (config.getOptional(option).isPresent()) {
                present.add(option.key());
            }
        }
        if (present.isEmpty()) {
            return;
        }
        // The key is named in the remedy rather than blamed for the mismatch: it may be absent,
        // in which case the connector's default is what selected the disposition and a message
        // pointing at a key the DDL never wrote would send the reader looking for it.
        throw new ValidationException(
                String.format(
                        "Options %s describe the column families the sink would create, but this"
                                + " table does not create any. Remove them, or set '%s' = '%s'.",
                        present,
                        BigtableConnectorOptions.SINK_CREATE_DISPOSITION.key(),
                        CreateDisposition.CREATE_IF_NEEDED));
    }

    private static GcRule gcRule(ReadableConfig config) {
        Optional<Integer> maxVersions =
                config.getOptional(BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_VERSIONS);
        Optional<Duration> maxAge =
                config.getOptional(BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_AGE);
        if (maxVersions.isPresent() && maxAge.isPresent()) {
            // A union, not an intersection: a cell goes when it is either too old or too far down
            // the version list, which is the shape Bigtable's own documentation recommends for a
            // family holding a bounded history.
            return GcRule.union(GcRule.maxVersions(maxVersions.get()), GcRule.maxAge(maxAge.get()));
        }
        if (maxVersions.isPresent()) {
            return GcRule.maxVersions(maxVersions.get());
        }
        if (maxAge.isPresent()) {
            return GcRule.maxAge(maxAge.get());
        }
        throw new ValidationException(
                String.format(
                        "'%s' = '%s' needs a garbage-collection rule for the families it creates:"
                                + " set '%s', '%s', or both. A family created without one keeps"
                                + " every version of every cell, and this sink is at-least-once,"
                                + " so a replayed row writes another version of the same cells.",
                        BigtableConnectorOptions.SINK_CREATE_DISPOSITION.key(),
                        CreateDisposition.CREATE_IF_NEEDED,
                        BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_VERSIONS.key(),
                        BigtableConnectorOptions.SINK_TABLE_CREATE_GC_RULE_MAX_AGE.key()));
    }
}
