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
 * The Flink 1.20 variant of the changelog-mode seam (selected by {@code -Dflink.compat=flink1}; see
 * the {@code src/main/java-flink2} twin for the full story).
 *
 * <p>1.20's {@code ChangelogMode} has no key-only-deletes concept at all — neither {@code
 * upsert(boolean)} nor {@code keyOnlyDeletes()} exists on it, verified against {@code
 * flink-table-common} 1.20.4 — so its planner already materialises a full row before every delete
 * it sends an upsert sink. That is the behaviour the flag asks for when it is {@code false}, and
 * there is no behaviour to ask for when it is {@code true}: the argument is therefore ignored here,
 * and 1.20 was never exposed to what this seam exists to fix.
 */
@Internal
final class CrossVersionChangelogMode {

    private CrossVersionChangelogMode() {}

    /**
     * Returns 1.20's only upsert changelog mode, whose deletes always carry the whole row.
     *
     * @param keyOnlyDeletes ignored on this build, which cannot express it
     * @return the mode to hand back from {@code getChangelogMode}
     */
    static ChangelogMode upsert(boolean keyOnlyDeletes) {
        return ChangelogMode.upsert();
    }
}
