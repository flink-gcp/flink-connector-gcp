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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Where a change-stream source starts reading when no checkpointed state is restored.
 *
 * <p>A configured position applies only to a fresh start. On restore, the source resumes from its
 * checkpointed per-partition positions instead. A source resolves this value once, when its split
 * enumerator starts, and validates it against the stream's retained history.
 *
 * <p>{@link #latest()} starts at the resolution instant and is the default used by change-stream
 * source builders. {@link #earliest()} starts at the oldest safely readable instant reported by the
 * source. {@link #at(Instant)} names an absolute instant, and {@link #ago(Duration)} names an
 * instant relative to resolution time.
 */
@PublicEvolving
public final class StartPosition implements Serializable {

    private static final long serialVersionUID = 1L;

    enum Kind {
        EARLIEST,
        LATEST,
        AT,
        AGO
    }

    private static final StartPosition EARLIEST = new StartPosition(Kind.EARLIEST, null, null);
    private static final StartPosition LATEST = new StartPosition(Kind.LATEST, null, null);

    private final Kind kind;
    @Nullable private final Instant instant;
    @Nullable private final Duration duration;

    private StartPosition(Kind kind, @Nullable Instant instant, @Nullable Duration duration) {
        this.kind = kind;
        this.instant = instant;
        this.duration = duration;
    }

    /** Returns a position at the oldest instant the change stream can safely serve. */
    public static StartPosition earliest() {
        return EARLIEST;
    }

    /** Returns a position at the instant the source enumerator starts. */
    public static StartPosition latest() {
        return LATEST;
    }

    /**
     * Returns a position at an absolute instant.
     *
     * @param instant the instant to start reading at
     * @return the start position
     */
    public static StartPosition at(Instant instant) {
        return new StartPosition(
                Kind.AT, Preconditions.checkNotNull(instant, "instant must not be null"), null);
    }

    /**
     * Returns a position the given duration before the source enumerator starts.
     *
     * @param duration how far before startup to begin; must be positive
     * @return the start position
     */
    public static StartPosition ago(Duration duration) {
        Preconditions.checkNotNull(duration, "duration must not be null");
        Preconditions.checkArgument(
                !duration.isZero() && !duration.isNegative(),
                "duration must be positive, but was %s",
                duration);
        return new StartPosition(Kind.AGO, null, duration);
    }

    Kind kind() {
        return kind;
    }

    Instant instant() {
        return Objects.requireNonNull(instant);
    }

    Duration duration() {
        return Objects.requireNonNull(duration);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartPosition that = (StartPosition) o;
        return kind == that.kind
                && Objects.equals(instant, that.instant)
                && Objects.equals(duration, that.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, instant, duration);
    }

    @Override
    public String toString() {
        switch (kind) {
            case AT:
                return "StartPosition{at=" + instant + "}";
            case AGO:
                return "StartPosition{ago=" + duration + "}";
            case EARLIEST:
                return "StartPosition{earliest}";
            case LATEST:
                return "StartPosition{latest}";
            default:
                throw new IllegalStateException("Unhandled start position kind " + kind + ".");
        }
    }
}
