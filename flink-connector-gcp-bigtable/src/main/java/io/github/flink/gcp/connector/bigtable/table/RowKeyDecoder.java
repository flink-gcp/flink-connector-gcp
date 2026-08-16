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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.ValidationException;

import com.google.protobuf.ByteString;

import java.util.Base64;

/** Decodes one configured row-key bound and rejects values the SDK would silently widen. */
@Internal
final class RowKeyDecoder {

    private RowKeyDecoder() {}

    static ByteString decode(
            ConfigOption<?> option, RowKeyEncoding encoding, String configuredValue) {
        ByteString decoded;
        switch (encoding) {
            case UTF8:
                decoded = ByteString.copyFromUtf8(configuredValue);
                break;
            case BASE64:
                decoded = decodeBase64(option, configuredValue);
                break;
            default:
                throw new IllegalStateException("Unknown row-key encoding: " + encoding);
        }

        if (decoded.isEmpty()) {
            throw emptyRowKey(option);
        }
        return decoded;
    }

    static ValidationException emptyRowKey(ConfigOption<?> option) {
        return new ValidationException(
                String.format(
                        "'%s' contains a value that decodes to an empty row key. An empty"
                                + " bound is not supported because the Bigtable client treats"
                                + " it as unbounded. Remove the empty value, or leave the option"
                                + " unset if no row-key bound is intended.",
                        option.key()));
    }

    static ByteString decodeBase64(ConfigOption<?> option, String configuredValue) {
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(configuredValue);
        } catch (IllegalArgumentException e) {
            throw invalidBase64(option, e);
        }

        if (!Base64.getEncoder().encodeToString(decoded).equals(configuredValue)) {
            throw invalidBase64(option, null);
        }
        return ByteString.copyFrom(decoded);
    }

    private static ValidationException invalidBase64(
            ConfigOption<?> option, IllegalArgumentException cause) {
        String message =
                String.format(
                        "'%s' must contain canonical padded RFC 4648 standard Base64. URL-safe"
                                + " characters, whitespace, missing or non-canonical padding, and"
                                + " malformed input are not accepted.",
                        option.key());
        return cause == null
                ? new ValidationException(message)
                : new ValidationException(message, cause);
    }
}
