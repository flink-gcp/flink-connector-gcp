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

package io.github.flink.gcp.connector.bigquery.sink.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/** Ordered physical fields appended to every destination row and table schema. */
@PublicEvolving
public final class AdditionalFields<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<AdditionalField<? super T>> fields;

    private AdditionalFields(Builder<T> builder) {
        Preconditions.checkArgument(
                !builder.fields.isEmpty(), "AdditionalFields requires at least one field");
        Set<String> names = new HashSet<>();
        for (AdditionalField<? super T> field : builder.fields) {
            Preconditions.checkArgument(
                    names.add(field.getName().toLowerCase(Locale.ROOT)),
                    "Duplicate additional field name: %s",
                    field.getName());
        }
        this.fields = Collections.unmodifiableList(new ArrayList<>(builder.fields));
    }

    /**
     * Creates an empty builder; at least one field must be added before {@link Builder#build()}.
     */
    public static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /** Returns the fields in descriptor, table-schema, and provider evaluation order. */
    public List<AdditionalField<? super T>> getFields() {
        return fields;
    }

    /** Builder for {@link AdditionalFields}. */
    @PublicEvolving
    public static final class Builder<T> {

        private final List<AdditionalField<? super T>> fields = new ArrayList<>();

        private Builder() {}

        /** Appends one physical field after fields already declared on this builder. */
        public Builder<T> field(AdditionalField<? super T> field) {
            fields.add(Preconditions.checkNotNull(field, "field must not be null"));
            return this;
        }

        /** Builds the immutable ordered field declarations. */
        public AdditionalFields<T> build() {
            return new AdditionalFields<>(this);
        }
    }
}
