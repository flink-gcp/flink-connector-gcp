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

package io.github.flink.gcp.connector.bigquery.sink.fileloads;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * In-memory {@link StagingStorage} fake mirroring GCS semantics: an object only becomes visible
 * when its stream is closed.
 */
final class InMemoryStagingStorage implements StagingStorage {

    private static final long serialVersionUID = 1L;

    private final Map<String, byte[]> objects = new LinkedHashMap<>();
    private final List<String> deleted = new ArrayList<>();

    @Override
    public OutputStream createObject(String gcsUri) {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                objects.put(gcsUri, toByteArray());
            }
        };
    }

    @Override
    public void deleteObjects(List<String> gcsUris) {
        for (String gcsUri : gcsUris) {
            objects.remove(gcsUri);
            deleted.add(gcsUri);
        }
    }

    Map<String, byte[]> getObjects() {
        return objects;
    }

    List<String> getDeleted() {
        return deleted;
    }
}
