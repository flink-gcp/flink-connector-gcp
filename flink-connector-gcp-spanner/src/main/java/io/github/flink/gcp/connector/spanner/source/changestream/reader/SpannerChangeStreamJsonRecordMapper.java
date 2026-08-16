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

package io.github.flink.gcp.connector.spanner.source.changestream.reader;

import org.apache.flink.annotation.Internal;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.Mod;
import io.github.flink.gcp.connector.spanner.source.changestream.ModType;
import io.github.flink.gcp.connector.spanner.source.changestream.ValueCaptureType;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/** Shared JSON-to-record contract for both dialect adapters. */
@Internal
final class SpannerChangeStreamJsonRecordMapper {

    private SpannerChangeStreamJsonRecordMapper() {}

    static SpannerChangeStreamRecord map(JsonObject envelope) throws IOException {
        if (envelope.size() != 1) {
            throw new IOException(
                    "Spanner Change Streams JSON must contain exactly one record member, but"
                            + " contained "
                            + envelope.size()
                            + ".");
        }
        if (envelope.has("data_change_record")) {
            return new SpannerChangeStreamRecord.Data(
                    data(only(envelope.get("data_change_record"))));
        }
        if (envelope.has("heartbeat_record")) {
            JsonObject heartbeat = only(envelope.get("heartbeat_record"));
            return new SpannerChangeStreamRecord.Heartbeat(instant(heartbeat, "timestamp"));
        }
        if (envelope.has("child_partitions_record")) {
            return children(only(envelope.get("child_partitions_record")));
        }
        throw new IOException("Spanner Change Streams row contains no supported record member.");
    }

    private static DataChangeRecord data(JsonObject value) throws IOException {
        List<DataChangeRecord.ColumnType> columns = new ArrayList<>();
        for (JsonElement element : array(value, "column_types")) {
            JsonObject column = element.getAsJsonObject();
            columns.add(
                    new DataChangeRecord.ColumnType(
                            string(column, "name"),
                            required(column, "type").toString(),
                            bool(column, "is_primary_key"),
                            number(column, "ordinal_position")));
        }

        List<Mod> mods = new ArrayList<>();
        for (JsonElement element : array(value, "mods")) {
            JsonObject mod = element.getAsJsonObject();
            mods.add(
                    new Mod(
                            required(mod, "keys").toString(),
                            optionalJson(mod, "new_values"),
                            optionalJson(mod, "old_values")));
        }
        try {
            return new DataChangeRecord(
                    instant(value, "commit_timestamp"),
                    string(value, "record_sequence"),
                    string(value, "server_transaction_id"),
                    bool(value, "is_last_record_in_transaction_in_partition"),
                    string(value, "table_name"),
                    columns,
                    mods,
                    ModType.valueOf(string(value, "mod_type")),
                    ValueCaptureType.valueOf(string(value, "value_capture_type")),
                    number(value, "number_of_records_in_transaction"),
                    number(value, "number_of_partitions_in_transaction"),
                    string(value, "transaction_tag"),
                    bool(value, "is_system_transaction"));
        } catch (IllegalArgumentException e) {
            throw new IOException("Spanner returned an unsupported data-change enum value.", e);
        }
    }

    private static SpannerChangeStreamRecord.Children children(JsonObject value)
            throws IOException {
        List<SpannerChangeStreamRecord.Child> children = new ArrayList<>();
        for (JsonElement element : array(value, "child_partitions")) {
            JsonObject child = element.getAsJsonObject();
            List<String> parents = new ArrayList<>();
            boolean initialParent = false;
            for (JsonElement parent : array(child, "parent_partition_tokens")) {
                if (parent.isJsonNull()) {
                    initialParent = true;
                } else {
                    parents.add(parent.getAsString());
                }
            }
            children.add(
                    new SpannerChangeStreamRecord.Child(
                            string(child, "token"), parents, initialParent));
        }
        return new SpannerChangeStreamRecord.Children(instant(value, "start_timestamp"), children);
    }

    private static JsonObject only(JsonElement value) throws IOException {
        if (value == null || value.isJsonNull()) {
            throw new IOException("Spanner Change Streams record member is null.");
        }
        if (value.isJsonArray()) {
            JsonArray array = value.getAsJsonArray();
            if (array.size() != 1) {
                throw new IOException(
                        "Spanner Change Streams record member must contain exactly one value, but"
                                + " contained "
                                + array.size()
                                + ".");
            }
            value = array.get(0);
        }
        if (!value.isJsonObject()) {
            throw new IOException("Spanner Change Streams record member is not an object.");
        }
        return value.getAsJsonObject();
    }

    private static JsonArray array(JsonObject object, String name) throws IOException {
        JsonElement value = required(object, name);
        if (!value.isJsonArray()) {
            throw new IOException("Spanner Change Streams member '" + name + "' is not an array.");
        }
        return value.getAsJsonArray();
    }

    private static JsonElement required(JsonObject object, String name) throws IOException {
        JsonElement value = object.get(name);
        if (value == null || value.isJsonNull()) {
            throw new IOException(
                    "Spanner Change Streams member '" + name + "' is missing or null.");
        }
        return value;
    }

    private static String string(JsonObject object, String name) throws IOException {
        return required(object, name).getAsString();
    }

    private static boolean bool(JsonObject object, String name) throws IOException {
        return required(object, name).getAsBoolean();
    }

    private static long number(JsonObject object, String name) throws IOException {
        return required(object, name).getAsLong();
    }

    private static Instant instant(JsonObject object, String name) throws IOException {
        try {
            return Instant.parse(string(object, name));
        } catch (DateTimeParseException e) {
            throw new IOException(
                    "Spanner Change Streams member '" + name + "' is not an instant.", e);
        }
    }

    private static String optionalJson(JsonObject object, String name) {
        JsonElement value = object.get(name);
        return value == null ? null : value.toString();
    }
}
