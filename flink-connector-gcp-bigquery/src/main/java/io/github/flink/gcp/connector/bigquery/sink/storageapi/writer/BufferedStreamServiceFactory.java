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

package io.github.flink.gcp.connector.bigquery.sink.storageapi.writer;

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.io.Serializable;

/**
 * Creates {@link BufferedStreamService} instances on the task manager. Serializable so sinks can
 * ship a factory (never a live client) into writers and committers.
 */
@Internal
public interface BufferedStreamServiceFactory extends Serializable {

    /**
     * Creates a service.
     *
     * @param location the BigQuery location routing hint for appends, or {@code null}
     * @return the service
     * @throws IOException if the underlying client cannot be created
     */
    BufferedStreamService create(String location) throws IOException;
}
