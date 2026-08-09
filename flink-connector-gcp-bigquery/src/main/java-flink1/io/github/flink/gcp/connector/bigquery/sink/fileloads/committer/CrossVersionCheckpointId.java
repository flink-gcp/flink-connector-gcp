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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;

/**
 * The Flink 1.20 variant of the checkpoint-id seam (selected by {@code -Dflink.compat=flink1};
 * see the {@code src/main/java-flink2} twin for the measured signatures on both majors): 1.20
 * declares {@code getCheckpointIdOrEOI()} abstract and returning {@code long}, while its {@code
 * getCheckpointId()} returns {@code OptionalLong}.
 *
 * <p>The deprecation this variant calls through does not become a removal for this project: 1.20
 * is the frozen 1.x LTS, so its API only takes patch releases. The announced removal is 2.x's,
 * and that is the major the twin keeps clear of it.
 */
@Internal
final class CrossVersionCheckpointId {

    private CrossVersionCheckpointId() {}

    /** Returns the checkpoint id the message carries. */
    @SuppressWarnings("deprecation")
    static long of(CommittableWithLineage<?> lineage) {
        return lineage.getCheckpointIdOrEOI();
    }
}
