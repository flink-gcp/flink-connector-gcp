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

import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.source.changestream.DataChangeRecord;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerChangeStreamDeserializationSchema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies both Change Streams runtime processes load the configured key file. */
class SpannerChangeStreamSourceCredentialsTest {

    private static final String MISSING_KEY = "/missing/spanner-service-account.json";

    @Test
    void coordinatorFactoryLoadsCredentialsOnTheJobManager() {
        assertCredentialFailure(() -> source().getConfig().getCoordinatorClientFactory().create());
    }

    @Test
    void queryFactoryLoadsCredentialsOnTheTaskManager() {
        assertCredentialFailure(() -> source().getConfig().getQueryClientFactory().create());
    }

    private static SpannerChangeStreamSource<Long> source() {
        return SpannerChangeStreamSource.<Long>builder()
                .database(SpannerDatabase.of("p", "i", "d"))
                .changeStreamName("changes")
                .deserializer(new NoOpDeserializer())
                .serviceAccountKeyFile(MISSING_KEY)
                .build();
    }

    private static void assertCredentialFailure(ThrowingCall call) {
        assertThatThrownBy(call::run)
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Spanner service-account key file.")
                .hasNoCause();
    }

    private static final class NoOpDeserializer
            implements SpannerChangeStreamDeserializationSchema<Long> {

        private static final long serialVersionUID = 1L;

        @Override
        public void deserialize(
                DataChangeRecord record, org.apache.flink.util.Collector<Long> out) {
            // No records are decoded in this credential-loading test.
        }

        @Override
        public TypeInformation<Long> getProducedType() {
            return TypeInformation.of(Long.class);
        }
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run() throws Exception;
    }
}
