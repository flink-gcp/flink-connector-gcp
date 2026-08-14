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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.PublicEvolving;

/** Policy for a BigQuery CDC destination table that already exists. */
@PublicEvolving
public enum CdcTableReconciliationPolicy {
    /**
     * Verify the CDC table contract without starting adoption or drift repair.
     *
     * <p>A matching connector-owned pending attempt is resumed so an interrupted operation can
     * finish.
     */
    VERIFY_ONLY("verify-only"),

    /** Reconcile mutable CDC table properties while rejecting primary-key drift. */
    RECONCILE("reconcile");

    private final String value;

    CdcTableReconciliationPolicy(String value) {
        this.value = value;
    }

    /** Returns the lower-case spelling used by the Table API option. */
    @Override
    public String toString() {
        return value;
    }
}
