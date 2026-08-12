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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Validates and normalizes JSON used by the public change-record values. */
@Internal
final class SpannerChangeStreamJsonNormalizer {

    private SpannerChangeStreamJsonNormalizer() {}

    static String normalizeValue(String json) {
        return sorted(JsonParser.parseString(json)).toString();
    }

    static String normalizeObject(String json, String name) {
        JsonElement parsed = JsonParser.parseString(json);
        Preconditions.checkArgument(parsed.isJsonObject(), "%s must contain a JSON object", name);
        return sorted(parsed).toString();
    }

    private static JsonElement sorted(JsonElement value) {
        if (value.isJsonObject()) {
            JsonObject source = value.getAsJsonObject();
            List<String> names = new ArrayList<>(source.keySet());
            Collections.sort(names);
            JsonObject result = new JsonObject();
            for (String name : names) {
                result.add(name, sorted(source.get(name)));
            }
            return result;
        }
        if (value.isJsonArray()) {
            JsonArray result = new JsonArray();
            for (JsonElement element : value.getAsJsonArray()) {
                result.add(sorted(element));
            }
            return result;
        }
        return value.deepCopy();
    }
}
