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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.regex.Pattern;

/** One physical BigQuery column appended after the configured serializer emits a row. */
@PublicEvolving
public final class AdditionalField<T> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Pattern PROTO_FIELD_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final String name;
    private final AdditionalFieldType type;
    private final AdditionalFieldNullPolicy nullPolicy;
    private final AdditionalFieldValueProvider<? super T> valueProvider;

    private AdditionalField(
            String name,
            AdditionalFieldType type,
            AdditionalFieldNullPolicy nullPolicy,
            AdditionalFieldValueProvider<? super T> valueProvider) {
        this.name = validateName(name);
        this.type = Preconditions.checkNotNull(type, "type must not be null");
        this.nullPolicy = Preconditions.checkNotNull(nullPolicy, "nullPolicy must not be null");
        this.valueProvider =
                Preconditions.checkNotNull(valueProvider, "valueProvider must not be null");
    }

    /** Creates an additional physical field with an explicit type and null policy. */
    public static <T> AdditionalField<T> of(
            String name,
            AdditionalFieldType type,
            AdditionalFieldNullPolicy nullPolicy,
            AdditionalFieldValueProvider<? super T> valueProvider) {
        return new AdditionalField<>(name, type, nullPolicy, valueProvider);
    }

    /** Returns the BigQuery column name. */
    public String getName() {
        return name;
    }

    /** Returns the logical BigQuery type. */
    public AdditionalFieldType getType() {
        return type;
    }

    /** Returns the field's null policy. */
    public AdditionalFieldNullPolicy getNullPolicy() {
        return nullPolicy;
    }

    /** Returns the serializable provider evaluated for each non-skipped record. */
    public AdditionalFieldValueProvider<? super T> getValueProvider() {
        return valueProvider;
    }

    private static String validateName(String name) {
        String checked = Preconditions.checkNotNull(name, "name must not be null");
        Preconditions.checkArgument(
                PROTO_FIELD_NAME.matcher(checked).matches(),
                "Additional field name must be a protobuf-compatible identifier: %s",
                checked);
        return checked;
    }
}
