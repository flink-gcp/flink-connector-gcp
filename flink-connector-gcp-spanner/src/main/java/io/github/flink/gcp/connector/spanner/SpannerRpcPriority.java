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

package io.github.flink.gcp.connector.spanner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;

import com.google.cloud.spanner.Options.RpcPriority;

/**
 * The priority Spanner schedules this connector's requests at.
 *
 * <p>Spanner uses the priority to decide what to shed when an instance is at capacity: a {@code
 * LOW} request yields to higher-priority traffic. Two cases this exists for: a streaming sink
 * competing with serving traffic on the same instance, and a batch read of a large table that must
 * not disturb it.
 *
 * <p>At the module root rather than under {@code sink}, because both directions take it — the same
 * reason {@link SpannerDatabase} is here.
 *
 * <p>This mirrors the client library's own priority enum rather than exposing it, so the public API
 * stays free of SDK types and a client upgrade cannot change what a job's configuration means.
 */
@PublicEvolving
public enum SpannerRpcPriority {

    /** Yields to other traffic on the instance. */
    LOW,

    /** Between the other two. */
    MEDIUM,

    /**
     * Competes with serving traffic — and what leaving the priority unset amounts to, since Spanner
     * defines {@code PRIORITY_UNSPECIFIED} as equivalent to {@code PRIORITY_HIGH}. So setting
     * {@link #MEDIUM} is a step down from the default, not a restatement of it.
     */
    HIGH;

    /**
     * Maps this connector's priority onto the client library's.
     *
     * <p>Written out rather than matched by enum name, so that a value added to either side fails
     * to compile here instead of silently changing what a job asked for. One copy, because both
     * directions need the mapping and two switches would drift.
     *
     * @return the client library's priority
     */
    @Internal
    public RpcPriority toSpanner() {
        switch (this) {
            case LOW:
                return RpcPriority.LOW;
            case MEDIUM:
                return RpcPriority.MEDIUM;
            case HIGH:
                return RpcPriority.HIGH;
            default:
                throw new IllegalStateException("Unsupported RPC priority: " + this);
        }
    }
}
