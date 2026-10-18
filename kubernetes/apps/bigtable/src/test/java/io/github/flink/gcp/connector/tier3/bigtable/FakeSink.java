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

package io.github.flink.gcp.connector.tier3.bigtable;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Records every element a writer receives, in this JVM, so a MiniCluster test can read what the
 * graph delivered to each of its connectors. It applies nothing and stages nothing.
 */
final class FakeSink<T> implements Sink<T> {
    private static final long serialVersionUID = 1L;
    private static final Map<String, ConcurrentLinkedQueue<Object>> RECEIVED =
            new ConcurrentHashMap<>();
    private final String id = UUID.randomUUID().toString();

    FakeSink() {
        RECEIVED.put(id, new ConcurrentLinkedQueue<>());
    }

    @SuppressWarnings("unchecked")
    List<T> received() {
        return (List<T>) List.copyOf(RECEIVED.get(id));
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) {
        var received = RECEIVED.get(id);
        return new SinkWriter<>() {
            @Override
            public void write(T element, Context context) {
                received.add(element);
            }

            @Override
            public void flush(boolean endOfInput) {}

            @Override
            public void close() {}
        };
    }
}
