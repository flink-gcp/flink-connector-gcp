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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.source.readrows.RowRanges;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Parses the table layer's compact union of closed-start, open-end row-key ranges. */
@Internal
final class RowRangeParser {

    private static final String OPTION = BigtableConnectorOptions.SCAN_ROW_RANGES.key();
    private static final String ESCAPABLE = "\\;,[]()";

    private RowRangeParser() {}

    static List<ByteStringRange> parse(RowKeyEncoding encoding, String configuredValue) {
        List<String> entries = splitEntries(configuredValue);
        List<ByteStringRange> ranges = new ArrayList<>(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            ranges.add(parseEntry(encoding, entries.get(i), i + 1));
        }
        return Collections.unmodifiableList(ranges);
    }

    private static List<String> splitEntries(String configuredValue) {
        List<String> entries = new ArrayList<>();
        StringBuilder entry = new StringBuilder();
        int entryNumber = 1;
        for (int i = 0; i < configuredValue.length(); i++) {
            char current = configuredValue.charAt(i);
            if (current == '\\') {
                if (i + 1 >= configuredValue.length()) {
                    throw invalid(entryNumber, "ends with an incomplete backslash escape");
                }
                char escaped = configuredValue.charAt(++i);
                if (ESCAPABLE.indexOf(escaped) < 0) {
                    throw invalid(
                            entryNumber,
                            String.format("uses an unsupported escape '\\\\%s'", escaped));
                }
                entry.append(current).append(escaped);
            } else if (current == ';') {
                entries.add(entry.toString());
                entry.setLength(0);
                entryNumber++;
            } else {
                entry.append(current);
            }
        }
        entries.add(entry.toString());
        return entries;
    }

    private static ByteStringRange parseEntry(
            RowKeyEncoding encoding, String entry, int entryNumber) {
        if (entry.isEmpty()) {
            throw invalid(entryNumber, "is empty");
        }
        if (entry.length() < 3
                || entry.charAt(0) != '['
                || entry.charAt(entry.length() - 1) != ')') {
            throw invalid(
                    entryNumber,
                    "must start with '[' and end with ')' to describe a closed-start, open-end"
                            + " range");
        }

        StringBuilder start = new StringBuilder();
        StringBuilder end = new StringBuilder();
        StringBuilder endpoint = start;
        boolean commaSeen = false;
        for (int i = 1; i < entry.length() - 1; i++) {
            char current = entry.charAt(i);
            if (current == '\\') {
                if (i + 1 >= entry.length() - 1) {
                    throw invalid(
                            entryNumber, "ends an endpoint with an incomplete backslash escape");
                }
                char escaped = entry.charAt(++i);
                if (ESCAPABLE.indexOf(escaped) < 0) {
                    throw invalid(
                            entryNumber,
                            String.format("uses an unsupported escape '\\\\%s'", escaped));
                }
                endpoint.append(escaped);
            } else if (current == ',') {
                if (commaSeen) {
                    throw invalid(entryNumber, "contains more than one unescaped comma");
                }
                commaSeen = true;
                endpoint = end;
            } else if (ESCAPABLE.indexOf(current) >= 0) {
                throw invalid(
                        entryNumber,
                        String.format("contains the unescaped grammar character '%s'", current));
            } else {
                endpoint.append(current);
            }
        }
        if (!commaSeen) {
            throw invalid(entryNumber, "must contain one unescaped comma between its endpoints");
        }
        if (start.length() == 0 && end.length() == 0) {
            throw invalid(entryNumber, "cannot leave both endpoints unbounded");
        }

        ByteString decodedStart = decode(encoding, start, entryNumber);
        ByteString decodedEnd = decode(encoding, end, entryNumber);
        if (decodedStart != null
                && decodedEnd != null
                && RowRanges.compareKeys(decodedStart, decodedEnd) >= 0) {
            throw invalid(
                    entryNumber,
                    decodedStart.equals(decodedEnd)
                            ? "has equal decoded endpoints"
                            : "has a decoded start greater than its end");
        }

        ByteStringRange range = ByteStringRange.unbounded();
        if (decodedStart != null) {
            range.startClosed(decodedStart);
        }
        if (decodedEnd != null) {
            range.endOpen(decodedEnd);
        }
        return range;
    }

    private static ByteString decode(
            RowKeyEncoding encoding, StringBuilder endpoint, int entryNumber) {
        if (endpoint.length() == 0) {
            return null;
        }
        try {
            return RowKeyDecoder.decode(
                    BigtableConnectorOptions.SCAN_ROW_RANGES, encoding, endpoint.toString());
        } catch (ValidationException e) {
            throw invalid(entryNumber, e.getMessage(), e);
        }
    }

    private static ValidationException invalid(int entryNumber, String detail) {
        return invalid(entryNumber, detail, null);
    }

    private static ValidationException invalid(
            int entryNumber, String detail, ValidationException cause) {
        String message = String.format("Invalid '%s' entry %d: %s.", OPTION, entryNumber, detail);
        return cause == null
                ? new ValidationException(message)
                : new ValidationException(message, cause);
    }
}
