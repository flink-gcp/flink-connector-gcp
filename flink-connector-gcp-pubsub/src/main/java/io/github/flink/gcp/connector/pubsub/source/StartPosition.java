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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Where the source starts consuming a subscription.
 *
 * <p>A Pub/Sub subscription has no offset a reader can resume from: its position <em>is</em> server
 * state, shared by every consumer. Anything other than {@link #continueFromSubscription()}
 * therefore works by seeking, which rewrites that shared state.
 *
 * <p><b>A seek affects every consumer of the subscription, including other jobs.</b> Use a
 * non-default start position only on a subscription the job owns.
 *
 * <p><b>The seek runs once, at the first start of a job, and never on a restore.</b> The enumerator
 * records that it ran in its checkpointed state, so a failover resumes rather than rewinding. A
 * redeploy <em>without</em> a savepoint has no such state, so it seeks again — as does a job that
 * crash-loops before its first checkpoint completes.
 *
 * <p>The shape here — a mode plus, for one mode, a timestamp — is what the Table API factory needs:
 * it mirrors Kafka's {@code scan.startup.mode} and {@code scan.startup.timestamp-millis} pair,
 * which {@link #of(Mode, Instant)} accepts directly. The static factories are the DataStream API.
 */
@PublicEvolving
public final class StartPosition implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Which starting point a {@link StartPosition} names. */
    @PublicEvolving
    public enum Mode {

        /**
         * Starts wherever the subscription already is, delivering whatever it has not acknowledged.
         * The default, and the only mode that issues no seek and so leaves other consumers alone.
         */
        CONTINUE_FROM_SUBSCRIPTION,

        /**
         * Starts from the oldest message the subscription still holds, replaying its whole retained
         * backlog.
         *
         * <p>How far back that reaches is a property of the subscription, not of this setting:
         * acknowledged messages are only replayable if the subscription retains them or its topic
         * does. Against a subscription with neither, this recovers only what was never
         * acknowledged.
         */
        EARLIEST_RETAINED,

        /**
         * Starts from messages published after the job starts, discarding the existing backlog by
         * marking it acknowledged.
         *
         * <p><b>This drops data</b> — everything already in the subscription. It also resolves
         * against the clock at the moment the seek runs, so it is the one mode that is not
         * reproducible: a failover before the source assigns any split seeks again, to a later
         * instant, discarding whatever arrived in between.
         */
        LATEST,

        /**
         * Starts from a given instant: messages published before it are marked acknowledged, those
         * published after it unacknowledged. Subject to the same retention limits as {@link
         * #EARLIEST_RETAINED} when the instant is in the past.
         */
        TIMESTAMP
    }

    private static final StartPosition CONTINUE =
            new StartPosition(Mode.CONTINUE_FROM_SUBSCRIPTION, null);
    private static final StartPosition EARLIEST = new StartPosition(Mode.EARLIEST_RETAINED, null);
    private static final StartPosition LATEST = new StartPosition(Mode.LATEST, null);

    private final Mode mode;
    @Nullable private final Instant timestamp;

    private StartPosition(Mode mode, @Nullable Instant timestamp) {
        this.mode = mode;
        this.timestamp = timestamp;
    }

    /** Starts wherever the subscription already is, without seeking. The default. */
    public static StartPosition continueFromSubscription() {
        return CONTINUE;
    }

    /** Starts from the oldest message the subscription still retains. */
    public static StartPosition earliestRetained() {
        return EARLIEST;
    }

    /** Starts from messages published after the job starts, discarding the existing backlog. */
    public static StartPosition latest() {
        return LATEST;
    }

    /**
     * Starts from the given instant.
     *
     * @param timestamp the publish time to start from
     * @return the start position
     */
    public static StartPosition fromTimestamp(Instant timestamp) {
        Preconditions.checkNotNull(timestamp, "timestamp must not be null");
        return new StartPosition(Mode.TIMESTAMP, timestamp);
    }

    /**
     * Builds a start position from a mode and an optional timestamp, the form a {@code
     * ConfigOption} pair produces. The static factories are the friendlier API for the DataStream
     * builder.
     *
     * @param mode the mode
     * @param timestamp the instant, required for {@link Mode#TIMESTAMP} and rejected for the others
     * @return the start position
     */
    public static StartPosition of(Mode mode, @Nullable Instant timestamp) {
        Preconditions.checkNotNull(mode, "mode must not be null");
        if (mode == Mode.TIMESTAMP) {
            Preconditions.checkArgument(
                    timestamp != null,
                    "A timestamp is required for start position mode TIMESTAMP.");
            return fromTimestamp(timestamp);
        }
        Preconditions.checkArgument(
                timestamp == null,
                "A timestamp is only meaningful for start position mode TIMESTAMP, but %s was given"
                        + " with mode %s.",
                timestamp,
                mode);
        switch (mode) {
            case CONTINUE_FROM_SUBSCRIPTION:
                return CONTINUE;
            case EARLIEST_RETAINED:
                return EARLIEST;
            case LATEST:
                return LATEST;
            default:
                throw new IllegalArgumentException("Unhandled start position mode " + mode + ".");
        }
    }

    /** Returns the mode. */
    public Mode getMode() {
        return mode;
    }

    /**
     * Returns the instant to start from, or {@code null} for every mode but {@link Mode#TIMESTAMP}.
     */
    @Nullable
    public Instant getTimestamp() {
        return timestamp;
    }

    /** Returns whether reaching this position requires seeking the subscription. */
    public boolean requiresSeek() {
        return mode != Mode.CONTINUE_FROM_SUBSCRIPTION;
    }

    /**
     * Resolves the instant to seek to.
     *
     * <p>{@link Mode#EARLIEST_RETAINED} resolves to the epoch: Pub/Sub documents a seek target
     * older than the retention window as marking every <em>retained</em> message unacknowledged,
     * which is exactly "as far back as this subscription goes" without having to ask how far that
     * is.
     *
     * @param now the moment the seek is being issued, which {@link Mode#LATEST} resolves against
     * @return the instant to seek to
     * @throws IllegalStateException if this position needs no seek
     */
    public Instant resolveSeekTime(Instant now) {
        Preconditions.checkNotNull(now, "now must not be null");
        switch (mode) {
            case EARLIEST_RETAINED:
                return Instant.EPOCH;
            case LATEST:
                return now;
            case TIMESTAMP:
                return timestamp;
            default:
                throw new IllegalStateException(
                        "Start position mode " + mode + " requires no seek.");
        }
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
        return mode == that.mode && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mode, timestamp);
    }

    @Override
    public String toString() {
        return mode == Mode.TIMESTAMP
                ? "StartPosition{mode=TIMESTAMP, timestamp=" + timestamp + "}"
                : "StartPosition{mode=" + mode + "}";
    }
}
