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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Struct;

import java.io.Serializable;

/** Point-read seam shared by synchronous and asynchronous lookup functions. */
@Internal
interface SpannerRowLookup extends Serializable, AutoCloseable {
    void open() throws Exception;

    Struct read(Key key);

    ApiFuture<Struct> readAsync(Key key);

    @Override
    void close() throws Exception;
}
