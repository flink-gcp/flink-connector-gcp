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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

/**
 * Reads the checkpoint id off a {@link CommittableWithLineage} — the cross-version seam the
 * FILE_LOADS pre-commit stage needs, kept out of shared source because no accessor spelling
 * compiles against both supported Flink majors (ADR-0054, refined by issue #404).
 *
 * <p>Two variants share this fully-qualified name, under {@code src/main/java-flink2} (this one)
 * and {@code src/main/java-flink1}; the build selects one via the {@code flink.compat} Maven
 * property (default {@code flink2}). Measured against both majors' artifacts on 2026-08-09:
 * Flink 2.x declares {@code getCheckpointId()} abstract, returning {@code long} and not
 * deprecated, and carries {@code getCheckpointIdOrEOI()} as a {@code @Deprecated(forRemoval =
 * true)} default delegating to it; Flink 1.20 declares {@code getCheckpointIdOrEOI()} abstract,
 * returning {@code long}, and {@code getCheckpointId()} as a default returning {@code
 * OptionalLong}. Both accessors are deprecated on 1.20 and only one of them is on 2.x, so the
 * only spelling shared source could use is the one 2.x has announced for removal.
 *
 * <p>Neither variant changes the value a stamper reads. On 2.x the deprecated accessor's whole
 * body is {@code return getCheckpointId();}, and on 1.20 the deprecated one is what {@code
 * CommittableWithLineage} implements — so each root calls exactly what one shared line called
 * before the split. This seam buys the compiler a spelling, not the connector a behaviour.
 *
 * <p>Unlike the {@code CrossVersionSink} bridge, this seam is not compile-only: it runs for every
 * message carrying lineage that the stamper maps.
 */
@Internal
final class CrossVersionCheckpointId {

    private CrossVersionCheckpointId() {}

    /** Returns the checkpoint id the message carries. */
    static long of(CommittableWithLineage<?> lineage) {
        return lineage.getCheckpointId();
    }
}
