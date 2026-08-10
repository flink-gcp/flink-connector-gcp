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

package io.github.flink.gcp.connector.bigtable.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.connector.ChangelogMode;

/**
 * A cross-version seam, alongside {@code
 * io.github.flink.gcp.connector.bigtable.sink.CrossVersionSink}: {@code
 * ChangelogMode.upsert(boolean)} and {@code ChangelogMode.keyOnlyDeletes()} exist on Flink 2.x and
 * not on the 1.20 LTS, so naming either in shared source — or in a test — breaks that build and not
 * this one.
 *
 * <p>This is the {@code src/main/java-flink2} variant, selected by the default {@code
 * flink.compat=flink2}; see the {@code java-flink1} twin for what 1.20 does instead.
 */
@Internal
final class CrossVersionChangelogMode {

    private CrossVersionChangelogMode() {}

    /**
     * Returns the upsert changelog mode, saying whether a {@code DELETE} may carry the upsert key
     * alone.
     *
     * @param keyOnlyDeletes whether the planner may send a delete carrying only the upsert key
     * @return the mode to hand back from {@code getChangelogMode}
     */
    static ChangelogMode upsert(boolean keyOnlyDeletes) {
        return ChangelogMode.upsert(keyOnlyDeletes);
    }
}
