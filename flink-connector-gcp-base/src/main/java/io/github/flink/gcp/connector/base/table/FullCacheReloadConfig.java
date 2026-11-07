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

package io.github.flink.gcp.connector.base.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger.ScheduleMode;
import org.apache.flink.table.connector.source.lookup.cache.trigger.TimedCacheReloadTrigger;

import io.github.flink.gcp.connector.base.options.OptionChecks;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Objects;

/**
 * The standard {@code lookup.full-cache.*} options of a table whose {@code lookup.cache} is {@code
 * FULL}, read and checked while the statement is planned, and the reload trigger they describe.
 *
 * <p>Every table connector with a full cache holds one, so the reload mapping and its planning
 * checks cannot drift between connectors (#2042). Whether a connector accepts {@code FULL} at all,
 * and which of its own options conflict with it, stays with the connector.
 */
@Internal
public final class FullCacheReloadConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final ReloadStrategy strategy;
    @Nullable private final Duration periodicReloadInterval;
    private final ScheduleMode periodicScheduleMode;
    @Nullable private final String timedReloadIsoTime;
    private final int timedReloadIntervalDays;

    private FullCacheReloadConfig(ReadableConfig config) {
        strategy = config.get(LookupOptions.FULL_CACHE_RELOAD_STRATEGY);
        periodicReloadInterval =
                config.getOptional(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL).orElse(null);
        periodicScheduleMode = config.get(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE);
        timedReloadIsoTime =
                config.getOptional(LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME).orElse(null);
        timedReloadIntervalDays =
                config.get(LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS);
        if (strategy == ReloadStrategy.PERIODIC && periodicReloadInterval != null) {
            // Flink's trigger checks only that the interval is positive and then schedules it in
            // whole milliseconds, so a sub-millisecond interval would plan and then fail as the job
            // starts.
            try {
                OptionChecks.checkAtLeastOneMilli(
                        periodicReloadInterval,
                        LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key());
            } catch (IllegalArgumentException e) {
                throw new ValidationException(e.getMessage(), e);
            }
        }
        // Let Flink's own triggers check the remaining combinations while the statement is
        // planned, not later on a TaskManager.
        try {
            createTrigger();
        } catch (DateTimeParseException e) {
            // Flink's timed trigger hands the value to DateTimeFormatter unwrapped, so the
            // failure would name the text but not the option it came from.
            throw new ValidationException(
                    String.format(
                            "Option '%s' must be an ISO-8601 time such as 10:15 or"
                                    + " 10:15:30+09:00, but was '%s'.",
                            LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME.key(),
                            timedReloadIsoTime),
                    e);
        }
    }

    /**
     * Reads and checks the full-cache reload options. Call it only for a table whose {@code
     * lookup.cache} is {@code FULL}: a missing periodic interval is refused here even though
     * another cache type would never read it.
     *
     * @param config the table's options
     * @return the reload options
     * @throws ValidationException if the periodic interval is shorter than one millisecond, or if
     *     the timed reload time is not an ISO-8601 time
     * @throws IllegalArgumentException if Flink's reload trigger refuses the options, such as a
     *     missing periodic interval
     */
    public static FullCacheReloadConfig from(ReadableConfig config) {
        return new FullCacheReloadConfig(config);
    }

    /**
     * Creates the reload trigger the options describe.
     *
     * @return a new reload trigger
     */
    public CacheReloadTrigger createTrigger() {
        Configuration config = asConfiguration();
        switch (strategy) {
            case PERIODIC:
                return PeriodicCacheReloadTrigger.fromConfig(config);
            case TIMED:
                return TimedCacheReloadTrigger.fromConfig(config);
            default:
                throw new IllegalStateException("Unknown full-cache reload strategy " + strategy);
        }
    }

    @VisibleForTesting
    Configuration asConfiguration() {
        Configuration config = new Configuration();
        // Flink's triggers refuse a configuration whose cache type is not FULL.
        config.set(LookupOptions.CACHE_TYPE, LookupCacheType.FULL);
        config.set(LookupOptions.FULL_CACHE_RELOAD_STRATEGY, strategy);
        setIfPresent(
                config, LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL, periodicReloadInterval);
        config.set(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE, periodicScheduleMode);
        setIfPresent(config, LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME, timedReloadIsoTime);
        config.set(LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS, timedReloadIntervalDays);
        return config;
    }

    private static <T> void setIfPresent(
            Configuration config, ConfigOption<T> option, @Nullable T value) {
        if (value != null) {
            config.set(option, value);
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FullCacheReloadConfig)) {
            return false;
        }
        FullCacheReloadConfig that = (FullCacheReloadConfig) other;
        return timedReloadIntervalDays == that.timedReloadIntervalDays
                && strategy == that.strategy
                && Objects.equals(periodicReloadInterval, that.periodicReloadInterval)
                && periodicScheduleMode == that.periodicScheduleMode
                && Objects.equals(timedReloadIsoTime, that.timedReloadIsoTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                strategy,
                periodicReloadInterval,
                periodicScheduleMode,
                timedReloadIsoTime,
                timedReloadIntervalDays);
    }
}
