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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigquery.sink.serializer.AdditionalFieldNullPolicy;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowAugmentationField;
import io.github.flink.gcp.connector.bigquery.sink.serializer.ProtoRowAugmentationField.WriteOnlyField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

/** Creates the internal write-only fields that represent BigQuery CDC pseudocolumns. */
@Internal
public final class CdcProtoRowFields {

    static final String CHANGE_TYPE_FIELD = "_change_type";
    static final String SEQUENCE_NUMBER_FIELD = "_change_sequence_number";

    private static final Pattern SEQUENCE_PATTERN =
            Pattern.compile("[0-9A-Fa-f]{1,16}(?:/[0-9A-Fa-f]{1,16}){0,3}");

    private CdcProtoRowFields() {}

    /** Returns the write-only fields consumed by the generic augmentation engine. */
    public static <T> List<ProtoRowAugmentationField<? super T>> create(
            CdcOptions<? super T> options) {
        List<ProtoRowAugmentationField<? super T>> fields = new ArrayList<>();
        fields.add(
                ProtoRowAugmentationField.writeOnly(
                        WriteOnlyField.CDC_CHANGE_TYPE,
                        AdditionalFieldNullPolicy.REQUIRED,
                        element -> {
                            CdcChangeType changeType =
                                    options.getChangeTypeProvider().getChangeType(element);
                            return changeType == null ? null : changeType.name();
                        },
                        "The CDC change type provider failed",
                        "The CDC change type provider returned null"));
        CdcSequenceNumberProvider<? super T> sequenceProvider = options.getSequenceNumberProvider();
        if (sequenceProvider != null) {
            fields.add(
                    ProtoRowAugmentationField.writeOnly(
                            WriteOnlyField.CDC_SEQUENCE_NUMBER,
                            AdditionalFieldNullPolicy.REQUIRED,
                            element ->
                                    normalizeSequence(sequenceProvider.getSequenceNumber(element)),
                            "The CDC sequence number provider failed",
                            "The CDC sequence number provider returned null"));
        }
        return Collections.unmodifiableList(fields);
    }

    private static String normalizeSequence(String sequenceNumber) throws IOException {
        if (sequenceNumber == null) {
            throw new IOException("The CDC sequence number provider returned null");
        }
        if (!SEQUENCE_PATTERN.matcher(sequenceNumber).matches()) {
            throw new IOException(
                    "A BigQuery CDC sequence number must contain one to four slash-separated"
                            + " hexadecimal sections of at most 16 digits each");
        }
        return sequenceNumber.toUpperCase(Locale.ROOT);
    }
}
