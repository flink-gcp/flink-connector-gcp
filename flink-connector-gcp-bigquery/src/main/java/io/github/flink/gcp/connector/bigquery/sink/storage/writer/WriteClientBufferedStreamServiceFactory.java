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

package io.github.flink.gcp.connector.bigquery.sink.storage.writer;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigquery.sink.storage.BufferedStreamOptions;

import java.io.IOException;

/**
 * Default {@link BufferedStreamServiceFactory} creating {@link WriteClientBufferedStreamService}s.
 */
@Internal
public final class WriteClientBufferedStreamServiceFactory implements BufferedStreamServiceFactory {

    private static final long serialVersionUID = 1L;

    @Override
    public BufferedStreamService create(String location, BufferedStreamOptions options)
            throws IOException {
        return new WriteClientBufferedStreamService(location, options);
    }
}
