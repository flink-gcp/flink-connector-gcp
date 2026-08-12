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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;

import com.google.cloud.spanner.Dialect;

import java.util.Locale;

/** Decodes one configured identifier component for Spanner catalog and data API names. */
@Internal
final class SpannerIdentifier {

    private SpannerIdentifier() {}

    static String configured(String value, Dialect dialect, String option) {
        try {
            return decode(value, dialect);
        } catch (IllegalArgumentException e) {
            throw new ValidationException(
                    option
                            + " must be one non-blank "
                            + dialectName(dialect)
                            + " identifier component using canonical quoting, but was '"
                            + value
                            + "'.",
                    e);
        }
    }

    private static String decode(String value, Dialect dialect) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("The identifier is blank.");
        }
        char quote = quote(dialect);
        if (value.charAt(0) == quote) {
            if (value.length() < 2 || value.charAt(value.length() - 1) != quote) {
                throw new IllegalArgumentException("The identifier has an unmatched quote.");
            }
            String catalogName = decodeQuoted(value.substring(1, value.length() - 1), dialect);
            if (catalogName.isEmpty()) {
                throw new IllegalArgumentException("The quoted identifier is empty.");
            }
            return catalogName;
        }
        if (value.indexOf('.') >= 0 || value.indexOf('`') >= 0 || value.indexOf('"') >= 0) {
            throw new IllegalArgumentException("The identifier is multipart or malformed.");
        }
        String catalogName = dialect == Dialect.POSTGRESQL ? value.toLowerCase(Locale.ROOT) : value;
        return catalogName;
    }

    private static String decodeQuoted(String body, Dialect dialect) {
        char quote = quote(dialect);
        StringBuilder decoded = new StringBuilder(body.length());
        for (int i = 0; i < body.length(); i++) {
            char current = body.charAt(i);
            if (dialect == Dialect.POSTGRESQL) {
                if (current != quote) {
                    decoded.append(current);
                    continue;
                }
                if (i + 1 >= body.length() || body.charAt(i + 1) != quote) {
                    throw new IllegalArgumentException("A quote is not doubled.");
                }
                decoded.append(quote);
                i++;
                continue;
            }
            if (current == quote) {
                throw new IllegalArgumentException("A backtick is not escaped.");
            }
            if (current != '\\') {
                decoded.append(current);
                continue;
            }
            if (++i >= body.length()) {
                throw new IllegalArgumentException("The identifier ends with an escape.");
            }
            char escaped = body.charAt(i);
            if (escaped != '\\' && escaped != quote) {
                throw new IllegalArgumentException(
                        "Only a backslash or backtick may be escaped in canonical quoting.");
            }
            decoded.append(escaped);
        }
        return decoded.toString();
    }

    private static char quote(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL ? '"' : '`';
    }

    private static String dialectName(Dialect dialect) {
        return dialect == Dialect.POSTGRESQL ? "PostgreSQL" : "GoogleSQL";
    }
}
