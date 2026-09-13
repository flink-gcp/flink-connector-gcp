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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigtable.sink.conditional.ConditionalFilter;
import io.github.flink.gcp.connector.bigtable.table.OptionSetters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Consumes the closed attribute vocabulary of one conditional DDL section. */
@Internal
final class ConditionalSettings {
    private final String prefix;
    private final Map<String, String> remaining;

    ConditionalSettings(String prefix, Map<String, String> values) {
        this.prefix = prefix;
        this.remaining = new HashMap<>(values);
    }

    String key(String attribute) {
        return prefix + attribute;
    }

    String optional(String attribute) {
        return remaining.remove(attribute);
    }

    String required(String attribute) {
        String value = optional(attribute);
        if (value == null) {
            throw error(attribute, "is required");
        }
        return value;
    }

    String family() {
        String family = required("family");
        OptionSetters.convert(key("family"), family, ConditionalFilter::familyEquals);
        return family;
    }

    void finish() {
        if (!remaining.isEmpty()) {
            throw error(new TreeSet<>(remaining.keySet()).first(), "is unknown or inapplicable");
        }
    }

    ValidationException error(String attribute, String detail) {
        return new ValidationException("Option '" + key(attribute) + "' " + detail + ".");
    }

    static List<ConditionalSettings> branch(String key, Map<String, String> values) {
        Map<Integer, Map<String, String>> groups = new TreeMap<>();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            String name = entry.getKey();
            int dot = name.indexOf('.');
            if (dot <= 0 || dot == name.length() - 1) {
                throw new ValidationException(
                        "Option '"
                                + key
                                + "."
                                + name
                                + "' must have the form <index>.<attribute>.");
            }
            String index = name.substring(0, dot);
            if (!index.matches("0|[1-9][0-9]*")) {
                throw new ValidationException(
                        "Option '"
                                + key
                                + "."
                                + name
                                + "' requires a canonical nonnegative index.");
            }
            int number;
            try {
                number = Integer.parseInt(index);
            } catch (NumberFormatException e) {
                throw new ValidationException(
                        "Option '" + key + "." + name + "' index is too large.");
            }
            groups.computeIfAbsent(number, ignored -> new HashMap<>())
                    .put(name.substring(dot + 1), entry.getValue());
        }
        if (groups.size() > 100_000) {
            throw new ValidationException("Option '" + key + "' permits at most 100000 mutations.");
        }
        List<ConditionalSettings> result = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, String>> group : groups.entrySet()) {
            if (group.getKey() != result.size()) {
                throw new ValidationException(
                        "Option '"
                                + key
                                + "."
                                + group.getKey()
                                + "' indexes must be consecutive from zero.");
            }
            result.add(new ConditionalSettings(key + "." + group.getKey() + ".", group.getValue()));
        }
        return result;
    }

    static long integer(String key, String value) {
        if (!value.matches("[+-]?[0-9]+")) {
            throw new ValidationException("Option '" + key + "' must be a signed BIGINT literal.");
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new ValidationException(
                    "Option '" + key + "' must be a signed BIGINT literal.", e);
        }
    }
}
