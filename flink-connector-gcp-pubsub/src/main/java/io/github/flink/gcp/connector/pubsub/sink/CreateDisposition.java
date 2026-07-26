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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Whether the sink may create destination topics that do not exist.
 *
 * <p>{@link #toString()} returns the hyphenated spelling rather than the constant name, because
 * that spelling is what a SQL {@code WITH} clause is written in: Flink resolves an enum {@code
 * ConfigOption} by matching the configured value against {@code toString()}, case-insensitively and
 * with no other normalization. Flink's own {@code DeliveryGuarantee} carries its option spelling
 * the same way.
 */
@PublicEvolving
public enum CreateDisposition {

    /** Create the destination topic with default topic settings when it does not exist. */
    CREATE_IF_NEEDED("create-if-needed"),

    /** Never create destination topics; publishing to a missing topic fails. */
    CREATE_NEVER("create-never");

    private final String value;

    CreateDisposition(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
