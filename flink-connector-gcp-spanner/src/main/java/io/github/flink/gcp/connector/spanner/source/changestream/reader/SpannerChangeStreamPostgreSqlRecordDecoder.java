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

import com.google.cloud.spanner.StructReader;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import java.io.IOException;

/** Decodes the JSON returned by a PostgreSQL-dialect Change Streams read function. */
@Internal
final class SpannerChangeStreamPostgreSqlRecordDecoder implements SpannerChangeStreamRecordDecoder {

    @Override
    public SpannerChangeStreamRecord decode(StructReader row) throws IOException {
        Type.Code code = row.getColumnType(0).getCode();
        String json;
        if (code == Type.Code.PG_JSONB) {
            json = row.getPgJsonb(0);
        } else if (code == Type.Code.JSON) {
            json = row.getJson(0);
        } else if (code == Type.Code.STRING) {
            json = row.getString(0);
        } else {
            throw new IOException(
                    "PostgreSQL Change Streams returned " + code + " instead of JSON.");
        }
        JsonElement parsed;
        try {
            parsed = JsonParser.parseString(json);
        } catch (RuntimeException e) {
            throw new IOException("PostgreSQL Change Streams returned malformed JSON.", e);
        }
        if (!parsed.isJsonObject()) {
            throw new IOException("PostgreSQL Change Streams JSON is not an object.");
        }
        return SpannerChangeStreamJsonRecordMapper.map(parsed.getAsJsonObject());
    }
}
