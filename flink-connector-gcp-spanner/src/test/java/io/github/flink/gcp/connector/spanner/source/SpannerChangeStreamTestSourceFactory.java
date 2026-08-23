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

package io.github.flink.gcp.connector.spanner.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.changestream.enumerator.SpannerChangeStreamCoordinatorClient;
import io.github.flink.gcp.connector.spanner.source.changestream.reader.ScriptedSpannerChangeStreamQueryClientFactory;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;

import java.time.Duration;
import java.time.Instant;

/** Builds service-free Change Stream sources whose savepoints can be restored by real clients. */
public final class SpannerChangeStreamTestSourceFactory {

    private SpannerChangeStreamTestSourceFactory() {}

    public static SpannerChangeStreamSource<String> staleSource(
            Instant startTimestamp, int partitions) {
        return SpannerChangeStreamSource.<String>builder()
                .database(DatabaseDestination.of("project", "instance", "database"))
                .changeStreamName("changes")
                .deserializer(new SequenceDeserializer())
                .startPosition(StartPosition.at(startTimestamp))
                .maxConcurrentQueriesPerSubtask(2)
                .coordinatorClientFactory(NoOpCoordinatorClient::new)
                .queryClientFactory(new ScriptedSpannerChangeStreamQueryClientFactory(partitions))
                .build();
    }

    private static final class NoOpCoordinatorClient
            implements SpannerChangeStreamCoordinatorClient {

        @Override
        public Duration initialize() {
            return Duration.ofDays(7);
        }

        @Override
        public void close() {}
    }

    private static final class SequenceDeserializer
            implements SpannerChangeStreamDeserializationSchema<String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(DataChangeRecord record, Collector<String> out) {
            out.collect(record.getRecordSequence());
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}
