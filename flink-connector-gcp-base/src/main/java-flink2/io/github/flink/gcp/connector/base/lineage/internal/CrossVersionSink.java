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

package io.github.flink.gcp.connector.base.lineage.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;

/**
 * The cross-version seam {@link TableLineageSink} implements instead of {@link Sink} directly.
 *
 * <p>Two variants exist, under {@code src/main/java-flink2} (this one, empty) and {@code
 * src/main/java-flink1}; the {@code flink.compat} Maven property selects one. Flink 1.20 still
 * declares the deprecated {@code createWriter(Sink.InitContext)} abstract while Flink 2.x removed
 * the type, so no single source file satisfies both compilers (ADR-0054). It is package-private
 * beside its only implementation; each connector module keeps its own public seam for its sinks.
 */
@Internal
interface CrossVersionSink<InputT> extends Sink<InputT> {}
