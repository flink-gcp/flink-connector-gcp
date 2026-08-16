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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

/**
 * The result of resolving a BigQuery destination for one record.
 *
 * <p>A resolver returns either a {@link TableDestination} for an ordinary write or an {@link
 * UnroutableRecord} for a record-specific routing failure. The constructor and visitor method are
 * package-private so the connector can exhaustively handle the supported result types.
 */
@Public
public abstract class DestinationResolution {

    DestinationResolution() {}

    abstract <T> void accept(
            T element,
            SinkWriter.Context context,
            DestinationResolutionDispatcher.Visitor<T> visitor)
            throws IOException;
}
