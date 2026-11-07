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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;

import io.github.flink.gcp.connector.base.table.FullCacheReloadConfig;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/** Lookup options owned by the Spanner table source. */
@Internal
public final class SpannerLookupConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean async;
    private final LookupCacheType cacheType;
    private final int maxRetries;
    @Nullable private final Duration expireAfterAccess;
    @Nullable private final Duration expireAfterWrite;
    private final boolean cacheMissingKey;
    @Nullable private final Long maxRows;
    @Nullable private final FullCacheReloadConfig fullReload;

    private SpannerLookupConfig(ReadableConfig config) {
        async = config.get(SpannerConnectorOptions.LOOKUP_ASYNC);
        cacheType = config.get(LookupOptions.CACHE_TYPE);
        maxRetries = config.get(LookupOptions.MAX_RETRIES);
        expireAfterAccess =
                config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS).orElse(null);
        expireAfterWrite =
                config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE).orElse(null);
        cacheMissingKey = config.get(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        maxRows = config.getOptional(LookupOptions.PARTIAL_CACHE_MAX_ROWS).orElse(null);
        if (maxRetries < 0) {
            throw new ValidationException("'lookup.max-retries' must be zero or greater.");
        }
        if (async && cacheType == LookupCacheType.FULL) {
            throw new ValidationException(
                    String.format(
                            "'%s' cannot be true when '%s' is FULL: Flink's full cache is"
                                    + " scan-backed and has no asynchronous lookup provider.",
                            SpannerConnectorOptions.LOOKUP_ASYNC.key(),
                            LookupOptions.CACHE_TYPE.key()));
        }
        if (maxRows != null && maxRows < 0) {
            throw new ValidationException(
                    "Option '"
                            + LookupOptions.PARTIAL_CACHE_MAX_ROWS.key()
                            + "' must be zero or greater.");
        }
        // Let Flink's own implementations validate the standard cache option combinations while
        // the DDL is planned, not later on a TaskManager.
        if (cacheType == LookupCacheType.PARTIAL) {
            createPartialCache();
        }
        // Read only for FULL, which is the only cache type that consults the reload options.
        fullReload = cacheType == LookupCacheType.FULL ? FullCacheReloadConfig.from(config) : null;
    }

    /**
     * Maps the lookup options from the factory's validated configuration.
     *
     * @param config the table's options
     * @return the lookup options
     */
    public static SpannerLookupConfig from(ReadableConfig config) {
        return new SpannerLookupConfig(config);
    }

    /**
     * Whether the source returns an asynchronous point-lookup provider.
     *
     * @return whether lookups are asynchronous
     */
    public boolean isAsync() {
        return async;
    }

    /**
     * The standard Flink cache mode.
     *
     * @return the cache mode
     */
    public LookupCacheType getCacheType() {
        return cacheType;
    }

    /**
     * The number of retries after the initial point read.
     *
     * @return the retry count
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * Creates the partial cache from Flink's standard options.
     *
     * @return a new partial cache
     */
    public DefaultLookupCache createPartialCache() {
        return DefaultLookupCache.fromConfig(partialCacheConfiguration());
    }

    /**
     * Creates the configured full-cache reload trigger.
     *
     * @return a new reload trigger
     */
    public CacheReloadTrigger createFullReloadTrigger() {
        if (fullReload == null) {
            throw new IllegalStateException(
                    "The full-cache reload trigger exists only when '"
                            + LookupOptions.CACHE_TYPE.key()
                            + "' is FULL, but it is "
                            + cacheType
                            + ".");
        }
        return fullReload.createTrigger();
    }

    private Configuration partialCacheConfiguration() {
        Configuration config = new Configuration();
        config.set(LookupOptions.CACHE_TYPE, cacheType);
        config.set(LookupOptions.MAX_RETRIES, maxRetries);
        setIfPresent(config, LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS, expireAfterAccess);
        setIfPresent(config, LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE, expireAfterWrite);
        config.set(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY, cacheMissingKey);
        setIfPresent(config, LookupOptions.PARTIAL_CACHE_MAX_ROWS, maxRows);
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
        if (!(other instanceof SpannerLookupConfig)) {
            return false;
        }
        SpannerLookupConfig that = (SpannerLookupConfig) other;
        return async == that.async
                && maxRetries == that.maxRetries
                && cacheMissingKey == that.cacheMissingKey
                && cacheType == that.cacheType
                && Objects.equals(expireAfterAccess, that.expireAfterAccess)
                && Objects.equals(expireAfterWrite, that.expireAfterWrite)
                && Objects.equals(maxRows, that.maxRows)
                && Objects.equals(fullReload, that.fullReload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                async,
                cacheType,
                maxRetries,
                expireAfterAccess,
                expireAfterWrite,
                cacheMissingKey,
                maxRows,
                fullReload);
    }
}
