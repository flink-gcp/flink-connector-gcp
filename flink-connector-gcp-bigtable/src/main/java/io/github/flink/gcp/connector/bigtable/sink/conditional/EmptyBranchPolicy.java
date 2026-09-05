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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.annotation.PublicEvolving;

/** Policy for a successful conditional request whose selected mutation list is empty. */
@PublicEvolving
public enum EmptyBranchPolicy {
    /** Accept the response, including a selected empty branch. */
    IGNORE("ignore"),
    /**
     * Fail the job after counting the successful response; a dropping handler cannot override it.
     */
    FAIL("fail");

    private final String value;

    EmptyBranchPolicy(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
