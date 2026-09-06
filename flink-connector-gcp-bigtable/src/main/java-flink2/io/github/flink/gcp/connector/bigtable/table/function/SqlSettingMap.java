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
import org.apache.flink.table.api.ValidationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/** Consumes a settings subtree and rejects attributes no template used. */
@Internal
final class SqlSettingMap {
    final String prefix;
    private final Map<String, String> remaining;

    SqlSettingMap(String prefix, Map<String, String> values) {
        this.prefix = prefix;
        remaining = new HashMap<>(values);
    }

    String key(String property) {
        return prefix + property;
    }

    String optional(String property) {
        return remaining.remove(property);
    }

    String required(String property) {
        String value = optional(property);
        if (value == null) {
            throw error(property, "is required");
        }
        return value;
    }

    String nonblank(String property) {
        String value = required(property);
        if (value.isBlank()) {
            throw error(property, "must not be blank");
        }
        return value;
    }

    SqlSettingMap section(String property) {
        String start = property + ".";
        Map<String, String> values = new HashMap<>();
        remaining
                .entrySet()
                .removeIf(
                        entry -> {
                            if (!entry.getKey().startsWith(start)) {
                                return false;
                            }
                            values.put(entry.getKey().substring(start.length()), entry.getValue());
                            return true;
                        });
        return new SqlSettingMap(key(start), values);
    }

    List<SqlSettingMap> numbered(String property) {
        SqlSettingMap section = section(property);
        Map<Integer, Map<String, String>> groups = new TreeMap<>();
        for (Map.Entry<String, String> entry : section.remaining.entrySet()) {
            String name = entry.getKey();
            int separator = name.indexOf('.');
            String index = separator < 0 ? name : name.substring(0, separator);
            int number = index(section.key(name), index);
            if (separator < 0 || separator == name.length() - 1) {
                throw section.error(name, "must name a property after its index");
            }
            groups.computeIfAbsent(number, ignored -> new HashMap<>())
                    .put(name.substring(separator + 1), entry.getValue());
        }
        List<SqlSettingMap> result = new ArrayList<>();
        for (Map.Entry<Integer, Map<String, String>> group : groups.entrySet()) {
            int index = group.getKey();
            if (index != result.size()) {
                String offendingProperty = new TreeSet<>(group.getValue().keySet()).first();
                throw section.error(
                        index + "." + offendingProperty, "indexes must be consecutive from zero");
            }
            result.add(new SqlSettingMap(section.key(index + "."), group.getValue()));
        }
        return result;
    }

    void finish() {
        if (!remaining.isEmpty()) {
            throw error(new TreeSet<>(remaining.keySet()).first(), "is unknown or inapplicable");
        }
    }

    ValidationException error(String property, String detail) {
        return new ValidationException("Option '" + key(property) + "' " + detail + ".");
    }

    static int index(String key, String value) {
        if (!value.matches("0|[1-9][0-9]*")) {
            throw new ValidationException(
                    "Option '" + key + "' must be a canonical nonnegative index.");
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new ValidationException("Option '" + key + "' index is too large.");
        }
    }

    static long integer(String key, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new ValidationException("Option '" + key + "' must be a signed BIGINT literal.");
        }
    }
}
