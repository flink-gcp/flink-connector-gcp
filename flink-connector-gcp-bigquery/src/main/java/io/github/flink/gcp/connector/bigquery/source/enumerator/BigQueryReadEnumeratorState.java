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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.bigquery.source.split.ReadStreamSplit;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The enumerator's checkpointed state: whether the read session exists, which one it is, and the
 * splits not currently assigned to a reader.
 *
 * <p>{@code initialized} is what keeps a restore from creating a second read session. A second
 * session would pin a second snapshot of the table, so a job that failed over would silently read a
 * table as of two different instants; the {@code readSessionsCreated} metric reports the same fact
 * at runtime, and any value above one means this flag failed.
 *
 * <p>The state holds no record of which subtask holds which split. Splits that were assigned come
 * back through {@code addSplitsBack} when their reader fails, and splits covered by a completed
 * checkpoint are restored by the reader that held them — so a per-subtask ledger here would be a
 * third account of the same fact, and the one that has to be reconciled with the other two.
 */
@Internal
public final class BigQueryReadEnumeratorState {

    private final boolean initialized;
    @Nullable private final String sessionName;
    @Nullable private final Instant sessionExpireTime;
    private final List<ReadStreamSplit> pendingSplits;

    /**
     * Creates the state.
     *
     * @param initialized whether the read session has been created
     * @param sessionName the read session's resource name, or {@code null} before it exists
     * @param sessionExpireTime when the read session expires, or {@code null} before it exists
     * @param pendingSplits the splits not currently assigned to a reader
     */
    public BigQueryReadEnumeratorState(
            boolean initialized,
            @Nullable String sessionName,
            @Nullable Instant sessionExpireTime,
            List<ReadStreamSplit> pendingSplits) {
        Preconditions.checkNotNull(pendingSplits, "pendingSplits must not be null");
        this.initialized = initialized;
        this.sessionName = sessionName;
        this.sessionExpireTime = sessionExpireTime;
        this.pendingSplits = Collections.unmodifiableList(new ArrayList<>(pendingSplits));
    }

    /** Returns whether the read session has been created. */
    public boolean isInitialized() {
        return initialized;
    }

    /** Returns the read session's resource name, or {@code null} before it exists. */
    @Nullable
    public String getSessionName() {
        return sessionName;
    }

    /** Returns when the read session expires, or {@code null} before it exists. */
    @Nullable
    public Instant getSessionExpireTime() {
        return sessionExpireTime;
    }

    /** Returns the splits not currently assigned to a reader. */
    public List<ReadStreamSplit> getPendingSplits() {
        return pendingSplits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BigQueryReadEnumeratorState)) {
            return false;
        }
        BigQueryReadEnumeratorState other = (BigQueryReadEnumeratorState) o;
        return initialized == other.initialized
                && Objects.equals(sessionName, other.sessionName)
                && Objects.equals(sessionExpireTime, other.sessionExpireTime)
                && pendingSplits.equals(other.pendingSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(initialized, sessionName, sessionExpireTime, pendingSplits);
    }

    @Override
    public String toString() {
        return "BigQueryReadEnumeratorState{initialized="
                + initialized
                + ", sessionName='"
                + sessionName
                + "', sessionExpireTime="
                + sessionExpireTime
                + ", pendingSplits="
                + pendingSplits.size()
                + '}';
    }
}
