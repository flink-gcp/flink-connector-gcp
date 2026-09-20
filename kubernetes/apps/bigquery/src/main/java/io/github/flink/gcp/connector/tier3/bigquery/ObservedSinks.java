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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryBufferedStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.BigQueryDefaultStreamSink;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.BufferedStreamServiceFactory;
import io.github.flink.gcp.connector.bigquery.sink.storage.writer.RowAppenderFactory;

/** Decorates runtime factories while retaining production writers, state and committers. */
@Internal
final class ObservedSinks {
    private ObservedSinks() {}

    @SuppressWarnings("unchecked")
    static Sink<Long> observe(Sink<Long> sink, RecoveryOptions options) {
        if (sink instanceof BigQueryBufferedStreamSink) {
            return new Buffered((BigQueryBufferedStreamSink<Long>) sink, options);
        }
        return new Default((BigQueryDefaultStreamSink<Long>) sink, options);
    }

    static final class Default extends BigQueryDefaultStreamSink<Long> {
        private static final long serialVersionUID = 1L;
        private final RecoveryOptions observationOptions;

        Default(BigQueryDefaultStreamSink<Long> sink, RecoveryOptions options) {
            super(sink.getConfig(), sink.getOptions());
            this.observationOptions = options;
        }

        @Override
        protected RowAppenderFactory createRowAppenderFactory(WriterInitContext context) {
            return AppenderObservations.logging(observationOptions, context)
                    .observe(super.createRowAppenderFactory(context));
        }
    }

    static final class Buffered extends BigQueryBufferedStreamSink<Long> {
        private static final long serialVersionUID = 1L;
        private final RecoveryOptions observationOptions;

        Buffered(BigQueryBufferedStreamSink<Long> sink, RecoveryOptions options) {
            super(sink.getConfig(), sink.getOptions());
            this.observationOptions = options;
        }

        @Override
        protected BufferedStreamServiceFactory createWriterServiceFactory(
                WriterInitContext context) {
            var observations = AppenderObservations.logging(observationOptions, context);
            var factory = super.createWriterServiceFactory(context);
            return (location, options) -> observations.observe(factory.create(location, options));
        }
    }
}
