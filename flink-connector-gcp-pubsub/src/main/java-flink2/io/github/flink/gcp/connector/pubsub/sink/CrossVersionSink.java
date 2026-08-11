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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.Sink;

/**
 * The cross-version seam every sink in this module implements instead of {@link Sink} directly.
 *
 * <p>Two variants of this interface exist under {@code src/main/java-flink2} (this one, empty)
 * and {@code src/main/java-flink1}; the build selects one via the {@code flink.compat} Maven
 * property (default {@code flink2}). Flink 1.20 still declares the deprecated {@code
 * createWriter(Sink.InitContext)} abstract while Flink 2.x removed the type outright, so no
 * single source file can satisfy both compilers — the 1.20 variant carries a compile-only default
 * for it, and everything else in the module stays one copy. See the version policy in the root
 * {@code AGENTS.md} and issue #32.
 */
@Internal
public interface CrossVersionSink<InputT> extends Sink<InputT> {}
