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

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/** Flush evidence before exposing an acknowledgement or injecting a failure. */
final class AcceptanceEvidence implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final FileChannel file;

    AcceptanceEvidence(Path path) throws IOException {
        file = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    synchronized void record(String event, Map<String, ?> values) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("event", event);
        row.put("observedAt", Instant.now().toString());
        row.putAll(values);
        try {
            byte[] bytes =
                    (JSON.writeValueAsString(row) + "\n")
                            .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) {
                file.write(buffer);
            }
            file.force(true);
        } catch (IOException failure) {
            throw new IllegalStateException("Cannot persist acceptance evidence", failure);
        }
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
