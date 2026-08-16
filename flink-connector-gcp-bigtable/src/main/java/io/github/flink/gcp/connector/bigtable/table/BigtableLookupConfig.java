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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger.ScheduleMode;
import org.apache.flink.table.connector.source.lookup.cache.trigger.TimedCacheReloadTrigger;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/** The lookup values a {@code CREATE TABLE} maps into the Bigtable table source. */
@Internal
public final class BigtableLookupConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean async;
    private final LookupCacheType cacheType;
    private final int maxRetries;
    @Nullable private final Duration partialExpireAfterAccess;
    @Nullable private final Duration partialExpireAfterWrite;
    private final boolean partialCacheMissingKey;
    @Nullable private final Long partialMaxRows;
    private final ReloadStrategy fullReloadStrategy;
    @Nullable private final Duration fullPeriodicReloadInterval;
    private final ScheduleMode fullPeriodicScheduleMode;
    @Nullable private final String fullTimedReloadIsoTime;
    private final int fullTimedReloadIntervalDays;

    private BigtableLookupConfig(ReadableConfig config) {
        this.async = config.get(BigtableConnectorOptions.LOOKUP_ASYNC);
        this.cacheType = config.get(LookupOptions.CACHE_TYPE);
        this.maxRetries = config.get(LookupOptions.MAX_RETRIES);
        this.partialExpireAfterAccess =
                config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS).orElse(null);
        this.partialExpireAfterWrite =
                config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE).orElse(null);
        this.partialCacheMissingKey = config.get(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        this.partialMaxRows = config.getOptional(LookupOptions.PARTIAL_CACHE_MAX_ROWS).orElse(null);
        this.fullReloadStrategy = config.get(LookupOptions.FULL_CACHE_RELOAD_STRATEGY);
        this.fullPeriodicReloadInterval =
                config.getOptional(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL).orElse(null);
        this.fullPeriodicScheduleMode =
                config.get(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE);
        this.fullTimedReloadIsoTime =
                config.getOptional(LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME).orElse(null);
        this.fullTimedReloadIntervalDays =
                config.get(LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS);

        if (maxRetries < 0) {
            throw new ValidationException(
                    String.format(
                            "'%s' must be zero or greater, but was %d.",
                            LookupOptions.MAX_RETRIES.key(), maxRetries));
        }
        if (async && cacheType == LookupCacheType.FULL) {
            throw new ValidationException(
                    String.format(
                            "'%s' cannot be true when '%s' is FULL: Flink's full cache is"
                                    + " scan-backed and has no asynchronous lookup provider.",
                            BigtableConnectorOptions.LOOKUP_ASYNC.key(),
                            LookupOptions.CACHE_TYPE.key()));
        }

        // Let Flink's own implementations validate the standard cache option combinations while
        // the DDL is planned, not later on a task manager.
        if (cacheType == LookupCacheType.PARTIAL) {
            createPartialCache();
        } else if (cacheType == LookupCacheType.FULL) {
            createFullReloadTrigger();
        }
    }

    /** Maps the lookup options from the factory's validated configuration. */
    public static BigtableLookupConfig from(ReadableConfig config) {
        return new BigtableLookupConfig(config);
    }

    /** Whether the source should return an asynchronous lookup provider. */
    public boolean isAsync() {
        return async;
    }

    /** The standard Flink cache mode. */
    public LookupCacheType getCacheType() {
        return cacheType;
    }

    /** The number of retries after the initial point read. */
    public int getMaxRetries() {
        return maxRetries;
    }

    /** Creates the partial cache from Flink's standard options. */
    public DefaultLookupCache createPartialCache() {
        return DefaultLookupCache.fromConfig(asConfiguration());
    }

    /** Creates the configured full-cache reload trigger. */
    public CacheReloadTrigger createFullReloadTrigger() {
        Configuration config = asConfiguration();
        switch (fullReloadStrategy) {
            case PERIODIC:
                return PeriodicCacheReloadTrigger.fromConfig(config);
            case TIMED:
                return TimedCacheReloadTrigger.fromConfig(config);
            default:
                throw new IllegalStateException(
                        "Unknown full-cache reload strategy " + fullReloadStrategy);
        }
    }

    @VisibleForTesting
    Configuration asConfiguration() {
        Configuration config = new Configuration();
        config.set(BigtableConnectorOptions.LOOKUP_ASYNC, async);
        config.set(LookupOptions.CACHE_TYPE, cacheType);
        config.set(LookupOptions.MAX_RETRIES, maxRetries);
        setIfPresent(
                config, LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS, partialExpireAfterAccess);
        setIfPresent(
                config, LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE, partialExpireAfterWrite);
        config.set(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY, partialCacheMissingKey);
        setIfPresent(config, LookupOptions.PARTIAL_CACHE_MAX_ROWS, partialMaxRows);
        config.set(LookupOptions.FULL_CACHE_RELOAD_STRATEGY, fullReloadStrategy);
        setIfPresent(
                config,
                LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL,
                fullPeriodicReloadInterval);
        config.set(
                LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE, fullPeriodicScheduleMode);
        setIfPresent(
                config, LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME, fullTimedReloadIsoTime);
        config.set(
                LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS,
                fullTimedReloadIntervalDays);
        return config;
    }

    private static <T> void setIfPresent(
            Configuration config,
            org.apache.flink.configuration.ConfigOption<T> option,
            @Nullable T value) {
        if (value != null) {
            config.set(option, value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BigtableLookupConfig)) {
            return false;
        }
        BigtableLookupConfig that = (BigtableLookupConfig) o;
        return async == that.async
                && maxRetries == that.maxRetries
                && partialCacheMissingKey == that.partialCacheMissingKey
                && fullTimedReloadIntervalDays == that.fullTimedReloadIntervalDays
                && cacheType == that.cacheType
                && Objects.equals(partialExpireAfterAccess, that.partialExpireAfterAccess)
                && Objects.equals(partialExpireAfterWrite, that.partialExpireAfterWrite)
                && Objects.equals(partialMaxRows, that.partialMaxRows)
                && fullReloadStrategy == that.fullReloadStrategy
                && Objects.equals(fullPeriodicReloadInterval, that.fullPeriodicReloadInterval)
                && fullPeriodicScheduleMode == that.fullPeriodicScheduleMode
                && Objects.equals(fullTimedReloadIsoTime, that.fullTimedReloadIsoTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                async,
                cacheType,
                maxRetries,
                partialExpireAfterAccess,
                partialExpireAfterWrite,
                partialCacheMissingKey,
                partialMaxRows,
                fullReloadStrategy,
                fullPeriodicReloadInterval,
                fullPeriodicScheduleMode,
                fullTimedReloadIsoTime,
                fullTimedReloadIntervalDays);
    }
}
