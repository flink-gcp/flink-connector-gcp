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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.annotation.Experimental;

/** Delivery modes for Cloud Tasks task creation, distinct from handler execution. */
@Experimental
public enum CloudTasksDeliveryGuarantee {
    /** Creates tasks eagerly; checkpoint replay can create another task. */
    AT_LEAST_ONCE("at-least-once"),
    /** Creates checkpointed named envelopes within the documented recovery window. */
    EXACTLY_ONCE("exactly-once");

    private final String value;

    CloudTasksDeliveryGuarantee(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
