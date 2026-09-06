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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Map;

/** Session settings consumed when an async SQL write is specialized. */
@Internal
public final class BigtableSqlFunctionOptions {
    private BigtableSqlFunctionOptions() {}

    /** Named connection and request definitions for Bigtable async SQL functions. */
    public static final ConfigOption<Map<String, String>> FUNCTIONS =
            ConfigOptions.key("bigtable.functions")
                    .mapType()
                    .noDefaultValue()
                    .withDescription(
                            "Named connection and request definitions for Bigtable async SQL functions.");
}
