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

package io.github.flink.gcp.connector.cloudtasks.table.form;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

/** Serializes STRING columns as an HTTP form in physical schema order. */
@Internal
final class FormUrlEncodedRowDataSerializationSchema implements SerializationSchema<RowData> {

    private static final long serialVersionUID = 1L;

    private final String[] fieldNames;
    private final String[] encodedFieldNames;
    private final boolean[] arrays;

    FormUrlEncodedRowDataSerializationSchema(RowType rowType) {
        int fieldCount = rowType.getFieldCount();
        this.fieldNames = new String[fieldCount];
        this.encodedFieldNames = new String[fieldCount];
        this.arrays = new boolean[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            String fieldName = rowType.getFieldNames().get(i);
            fieldNames[i] = fieldName;
            encodedFieldNames[i] = encode(fieldName);
            arrays[i] = rowType.getTypeAt(i).getTypeRoot() == LogicalTypeRoot.ARRAY;
        }
    }

    @Override
    public byte[] serialize(RowData row) {
        StringBuilder form = new StringBuilder();
        for (int field = 0; field < fieldNames.length; field++) {
            if (row.isNullAt(field)) {
                continue;
            }
            if (arrays[field]) {
                appendArray(form, row.getArray(field), field);
            } else {
                appendField(form, encodedFieldNames[field], row.getString(field).toString());
            }
        }
        return form.toString().getBytes(StandardCharsets.UTF_8);
    }

    private void appendArray(StringBuilder form, ArrayData values, int field) {
        for (int element = 0; element < values.size(); element++) {
            if (values.isNullAt(element)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Form field '%s' has a null array element at index %d;"
                                        + " application/x-www-form-urlencoded cannot represent it.",
                                fieldNames[field], element));
            }
            appendField(form, encodedFieldNames[field], values.getString(element).toString());
        }
    }

    private static void appendField(StringBuilder form, String encodedName, String value) {
        if (form.length() != 0) {
            form.append('&');
        }
        form.append(encodedName).append('=').append(encode(value));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
