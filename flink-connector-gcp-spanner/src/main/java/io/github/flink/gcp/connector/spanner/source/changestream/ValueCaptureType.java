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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.PublicEvolving;

/**
 * The value-capture policy that was active when a data change was recorded. Together with the
 * operation, it decides which old and new values a record's {@link Mod}s carry: a delete carries no
 * new values under any policy, and primary-key values are always carried in each mod's keys member.
 */
@PublicEvolving
public enum ValueCaptureType {

    /** Captures the old and new values of the modified columns. */
    OLD_AND_NEW_VALUES,

    /** Captures only the new values of the modified non-key columns, and no old values. */
    NEW_VALUES,

    /** Captures the new values of every watched column, modified or not, and no old values. */
    NEW_ROW,

    /**
     * Captures the new values of every watched column plus the old values of the modified columns.
     */
    NEW_ROW_AND_OLD_VALUES
}
