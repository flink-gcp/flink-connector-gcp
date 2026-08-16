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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.StructReader;
import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Decodes the nested STRUCT returned by a GoogleSQL Change Streams read function. */
@Internal
final class SpannerChangeStreamGoogleSqlRecordDecoder implements SpannerChangeStreamRecordDecoder {

    @Override
    public SpannerChangeStreamRecord decode(StructReader row) throws IOException {
        List<Struct> envelopes = row.getStructList(0);
        if (envelopes.size() != 1) {
            throw new IOException(
                    "GoogleSQL Change Streams row must contain one ChangeRecord, but contained "
                            + envelopes.size()
                            + ".");
        }
        Struct envelope = envelopes.get(0);
        JsonObject json = new JsonObject();
        int populated = 0;
        populated += addRecord(json, envelope, "data_change_record", this::data);
        populated += addRecord(json, envelope, "heartbeat_record", this::heartbeat);
        populated += addRecord(json, envelope, "child_partitions_record", this::children);
        if (populated != 1) {
            throw new IOException(
                    "GoogleSQL Change Streams row must contain exactly one record kind, but"
                            + " contained "
                            + populated
                            + ".");
        }
        return SpannerChangeStreamJsonRecordMapper.map(json);
    }

    private int addRecord(JsonObject target, Struct envelope, String name, StructJsonMapper mapper)
            throws IOException {
        List<Struct> records =
                envelope.isNull(name) ? Collections.emptyList() : envelope.getStructList(name);
        if (records.isEmpty()) {
            return 0;
        }
        if (records.size() != 1) {
            throw new IOException(
                    "GoogleSQL Change Streams member '"
                            + name
                            + "' contained "
                            + records.size()
                            + " values.");
        }
        target.add(name, mapper.map(records.get(0)));
        return 1;
    }

    private JsonObject data(Struct value) throws IOException {
        JsonObject result = new JsonObject();
        result.addProperty("commit_timestamp", instant(value.getTimestamp("commit_timestamp")));
        copyString(result, value, "record_sequence");
        copyString(result, value, "server_transaction_id");
        copyBoolean(result, value, "is_last_record_in_transaction_in_partition");
        copyString(result, value, "table_name");

        JsonArray columns = new JsonArray();
        for (Struct column : value.getStructList("column_types")) {
            JsonObject converted = new JsonObject();
            copyString(converted, column, "name");
            converted.add("type", JsonParser.parseString(column.getJson("type")));
            copyBoolean(converted, column, "is_primary_key");
            copyLong(converted, column, "ordinal_position");
            columns.add(converted);
        }
        result.add("column_types", columns);

        JsonArray mods = new JsonArray();
        for (Struct mod : value.getStructList("mods")) {
            JsonObject converted = new JsonObject();
            copyJson(converted, mod, "keys");
            copyJson(converted, mod, "new_values");
            copyJson(converted, mod, "old_values");
            mods.add(converted);
        }
        result.add("mods", mods);
        copyString(result, value, "mod_type");
        copyString(result, value, "value_capture_type");
        copyLong(result, value, "number_of_records_in_transaction");
        copyLong(result, value, "number_of_partitions_in_transaction");
        copyString(result, value, "transaction_tag");
        copyBoolean(result, value, "is_system_transaction");
        return result;
    }

    private JsonObject heartbeat(Struct value) {
        JsonObject result = new JsonObject();
        result.addProperty("timestamp", instant(value.getTimestamp("timestamp")));
        return result;
    }

    private JsonObject children(Struct value) {
        JsonObject result = new JsonObject();
        result.addProperty("start_timestamp", instant(value.getTimestamp("start_timestamp")));
        copyString(result, value, "record_sequence");
        JsonArray children = new JsonArray();
        for (Struct child : value.getStructList("child_partitions")) {
            JsonObject converted = new JsonObject();
            copyString(converted, child, "token");
            JsonArray parents = new JsonArray();
            for (String parent : child.getStringList("parent_partition_tokens")) {
                if (parent == null) {
                    parents.add(JsonNull.INSTANCE);
                } else {
                    parents.add(parent);
                }
            }
            converted.add("parent_partition_tokens", parents);
            children.add(converted);
        }
        result.add("child_partitions", children);
        return result;
    }

    private static void copyString(JsonObject target, Struct source, String name) {
        if (source.isNull(name)) {
            target.add(name, JsonNull.INSTANCE);
        } else {
            target.addProperty(name, source.getString(name));
        }
    }

    private static void copyBoolean(JsonObject target, Struct source, String name) {
        if (source.isNull(name)) {
            target.add(name, JsonNull.INSTANCE);
        } else {
            target.addProperty(name, source.getBoolean(name));
        }
    }

    private static void copyLong(JsonObject target, Struct source, String name) {
        if (source.isNull(name)) {
            target.add(name, JsonNull.INSTANCE);
        } else {
            target.addProperty(name, source.getLong(name));
        }
    }

    private static void copyJson(JsonObject target, Struct source, String name) {
        if (!source.isNull(name)) {
            target.add(name, JsonParser.parseString(source.getJson(name)));
        }
    }

    private static String instant(Timestamp timestamp) {
        return java.time.Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos())
                .toString();
    }

    @FunctionalInterface
    private interface StructJsonMapper {
        JsonObject map(Struct value) throws IOException;
    }
}
