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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Resolves change-stream start positions against one startup instant and one retention window.
 *
 * <p>The retention lookup is lazy: a fresh {@link StartPosition#latest()} needs no retention
 * permission or service call. Every other fresh position and every restored position needs the
 * computed earliest instant, so the lookup runs once and its result is reused. The one-minute
 * safety margin keeps the first read away from a retention boundary that continues moving between
 * resolution and admission by the service.
 *
 * <p>A connector creates one resolver when its enumerator starts and passes the enumerator class as
 * the log owner. Warnings then remain under the connector's logger category rather than this shared
 * helper's category.
 */
@Internal
public final class StartPositionResolver {

    static final Duration RETENTION_SAFETY_MARGIN = Duration.ofMinutes(1);

    /** Validates a retention duration against the resolver's moving-boundary safety margin. */
    public static void validateRetention(Duration retention, String name) {
        Preconditions.checkNotNull(retention, "%s must not be null", name);
        Preconditions.checkArgument(
                retention.compareTo(RETENTION_SAFETY_MARGIN) > 0,
                "%s must be longer than the %s retention safety margin, but was %s",
                name,
                RETENTION_SAFETY_MARGIN,
                retention);
    }

    private final Instant now;
    private final RetentionLookup retentionLookup;
    private final Logger log;

    @Nullable private Instant earliest;

    /**
     * Discovers how long a connector's change stream retains records.
     *
     * <p>Bigtable reads the table's change-stream configuration; Spanner reads the change-stream
     * options information schema and applies its configured absent-row default.
     */
    @FunctionalInterface
    public interface RetentionLookup {

        /** Returns the stream's effective retention period. */
        Duration get() throws Exception;
    }

    /**
     * Creates a resolver whose startup instant is the current UTC instant.
     *
     * @param logOwner the connector class whose logger should carry resolution warnings
     * @param retentionLookup the connector-specific retention lookup
     * @return a resolver for one enumerator startup
     */
    public static StartPositionResolver create(Class<?> logOwner, RetentionLookup retentionLookup) {
        return create(logOwner, retentionLookup, Clock.systemUTC());
    }

    @VisibleForTesting
    static StartPositionResolver create(
            Class<?> logOwner, RetentionLookup retentionLookup, Clock clock) {
        Preconditions.checkNotNull(logOwner, "logOwner must not be null");
        Preconditions.checkNotNull(retentionLookup, "retentionLookup must not be null");
        Preconditions.checkNotNull(clock, "clock must not be null");
        return new StartPositionResolver(
                clock.instant(), retentionLookup, LoggerFactory.getLogger(logOwner));
    }

    private StartPositionResolver(Instant now, RetentionLookup retentionLookup, Logger log) {
        this.now = now;
        this.retentionLookup = retentionLookup;
        this.log = log;
    }

    /**
     * Resolves the position configured for a fresh start.
     *
     * <p>An absolute position in the future is rejected. A position before the computed earliest is
     * clamped to the earliest and reported as a warning naming the unavailable range.
     *
     * @param requested the configured start position
     * @return the absolute instant to start reading at
     * @throws Exception if retention discovery fails
     */
    public Instant resolve(StartPosition requested) throws Exception {
        Preconditions.checkNotNull(requested, "requested must not be null");
        return resolve(requested, true);
    }

    /**
     * Checks one restored partition against the retained window.
     *
     * <p>An empty result means the restored state remains valid and must win over the configured
     * fresh-start position. A present result means the state expired and the affected partition
     * must restart at the returned, resolved fallback position. Without a fallback, expiry fails
     * the job rather than silently advancing over unavailable records.
     *
     * @param partition the connector's stable description of the restored partition
     * @param restoredPosition the partition's checkpointed read position or low watermark
     * @param fallback the explicitly configured fallback, or {@code null} when none was configured
     * @return empty to retain restored state, or the fallback instant to restart from
     * @throws Exception if retention discovery fails
     */
    public Optional<Instant> resolveRestored(
            String partition, Instant restoredPosition, @Nullable StartPosition fallback)
            throws Exception {
        Optional<RestoreExpiry> expiry = inspectRestored(partition, restoredPosition);
        if (!expiry.isPresent()) {
            return Optional.empty();
        }

        RestoreExpiry expired = expiry.get();
        if (fallback == null) {
            throw expired.asFailure();
        }

        Instant resolvedFallback = resolveFallback(fallback);
        log.warn(
                "Restored change-stream position {} for partition {} is older than the computed"
                        + " earliest position {}; the unavailable range is {}. Restarting that"
                        + " partition from fallback {} resolved to {}.",
                expired.getRestoredPosition(),
                expired.getPartition(),
                expired.getComputedEarliest(),
                expired.getUnavailableRange(),
                fallback,
                resolvedFallback);
        return Optional.of(resolvedFallback);
    }

    /**
     * Inspects one restored partition without choosing how the connector recovers from expiry.
     *
     * <p>Most connectors can use {@link #resolveRestored} directly. A connector whose partition
     * topology must be restarted as one unit can inspect every unfinished partition first and then
     * make one recovery decision for the whole ledger.
     *
     * @param partition the connector's stable description of the restored partition
     * @param restoredPosition the partition's checkpointed read position or low watermark
     * @return the expiry details, or empty when the position remains retained
     * @throws Exception if retention discovery fails
     */
    public Optional<RestoreExpiry> inspectRestored(String partition, Instant restoredPosition)
            throws Exception {
        Preconditions.checkNotNull(partition, "partition must not be null");
        Preconditions.checkArgument(!partition.isEmpty(), "partition must not be empty");
        Preconditions.checkNotNull(restoredPosition, "restoredPosition must not be null");

        Instant computedEarliest = earliest();
        if (!restoredPosition.isBefore(computedEarliest)) {
            return Optional.empty();
        }

        return Optional.of(new RestoreExpiry(partition, restoredPosition, computedEarliest));
    }

    /**
     * Resolves an explicitly configured restore fallback without emitting a fresh-start warning.
     */
    public Instant resolveFallback(StartPosition fallback) throws Exception {
        Preconditions.checkNotNull(fallback, "fallback must not be null");
        return resolve(fallback, false);
    }

    private Instant resolve(StartPosition requested, boolean warnOnClamp) throws Exception {
        Instant resolved;
        switch (requested.kind()) {
            case EARLIEST:
                return earliest();
            case LATEST:
                return now;
            case AT:
                resolved = requested.instant();
                break;
            case AGO:
                try {
                    resolved = now.minus(requested.duration());
                } catch (DateTimeException | ArithmeticException e) {
                    throw new IllegalArgumentException(
                            "Start position "
                                    + requested
                                    + " cannot be represented relative to startup instant "
                                    + now
                                    + ".",
                            e);
                }
                break;
            default:
                throw new IllegalStateException(
                        "Unhandled start position kind " + requested.kind() + ".");
        }

        if (resolved.isAfter(now)) {
            throw new IllegalArgumentException(
                    "Change-stream start position "
                            + resolved
                            + " is after the enumerator startup instant "
                            + now
                            + ". Choose latest() or an instant that is not in the future.");
        }

        Instant computedEarliest = earliest();
        if (!resolved.isBefore(computedEarliest)) {
            return resolved;
        }

        if (warnOnClamp) {
            log.warn(
                    "Requested change-stream start position {} is older than the computed earliest"
                            + " position {}; the unavailable range is {}. Starting at the computed"
                            + " earliest position.",
                    resolved,
                    computedEarliest,
                    Duration.between(resolved, computedEarliest));
        }
        return computedEarliest;
    }

    private Instant earliest() throws Exception {
        if (earliest != null) {
            return earliest;
        }

        Duration retention = retentionLookup.get();
        Preconditions.checkNotNull(retention, "retentionLookup must not return null");
        validateRetention(retention, "retention");
        try {
            earliest = now.minus(retention).plus(RETENTION_SAFETY_MARGIN);
        } catch (DateTimeException | ArithmeticException e) {
            throw new IllegalArgumentException(
                    "Retention "
                            + retention
                            + " cannot be represented relative to startup instant "
                            + now
                            + ".",
                    e);
        }
        return earliest;
    }

    /** The retained-window evidence for one expired restored partition. */
    @Internal
    public static final class RestoreExpiry {

        private final String partition;
        private final Instant restoredPosition;
        private final Instant computedEarliest;
        private final Duration unavailableRange;

        private RestoreExpiry(
                String partition, Instant restoredPosition, Instant computedEarliest) {
            this.partition = partition;
            this.restoredPosition = restoredPosition;
            this.computedEarliest = computedEarliest;
            this.unavailableRange = Duration.between(restoredPosition, computedEarliest);
        }

        public String getPartition() {
            return partition;
        }

        public Instant getRestoredPosition() {
            return restoredPosition;
        }

        public Instant getComputedEarliest() {
            return computedEarliest;
        }

        public Duration getUnavailableRange() {
            return unavailableRange;
        }

        public FlinkRuntimeException asFailure() {
            return new FlinkRuntimeException(
                    "Restored change-stream position "
                            + restoredPosition
                            + " for partition "
                            + partition
                            + " is older than the computed earliest position "
                            + computedEarliest
                            + "; the unavailable range is "
                            + unavailableRange
                            + ". Restart the job without restored state and choose a StartPosition"
                            + " known to be retained.");
        }
    }
}
