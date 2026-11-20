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

package io.github.flink.gcp.connector.datastore.table;

import org.apache.flink.annotation.Internal;
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

/**
 * The {@code lookup.*} options of a {@code datastore} table, read and checked when the statement is
 * planned.
 *
 * <p>Flink owns every cache. A {@code FULL} cache is loaded by a scan of the table, so it has no
 * asynchronous provider and {@code lookup.async} is refused with it; its {@code
 * lookup.full-cache.*} options are read only for {@code FULL}, through {@link
 * FullCacheReloadConfig}.
 */
@Internal
public final class DatastoreLookupConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean async;
    private final LookupCacheType cacheType;
    private final int maxRetries;
    @Nullable private final Duration expireAfterAccess;
    @Nullable private final Duration expireAfterWrite;
    private final boolean cacheMissingKey;
    @Nullable private final Long maxRows;
    @Nullable private final FullCacheReloadConfig fullReload;

    private DatastoreLookupConfig(ReadableConfig config) {
        async = config.get(DatastoreConnectorOptions.LOOKUP_ASYNC);
        cacheType = config.get(LookupOptions.CACHE_TYPE);
        maxRetries = config.get(LookupOptions.MAX_RETRIES);
        expireAfterAccess =
                config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS).orElse(null);
        expireAfterWrite =
                config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE).orElse(null);
        cacheMissingKey = config.get(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        maxRows = config.getOptional(LookupOptions.PARTIAL_CACHE_MAX_ROWS).orElse(null);
        if (async && cacheType == LookupCacheType.FULL) {
            throw new ValidationException(
                    String.format(
                            "'%s' cannot be true when '%s' is FULL: Flink's full cache is"
                                    + " scan-backed and has no asynchronous lookup provider.",
                            DatastoreConnectorOptions.LOOKUP_ASYNC.key(),
                            LookupOptions.CACHE_TYPE.key()));
        }
        if (maxRows != null && maxRows < 0) {
            // Flink checks the value only when a subtask opens the cache.
            throw new ValidationException(
                    "Option '"
                            + LookupOptions.PARTIAL_CACHE_MAX_ROWS.key()
                            + "' must be zero or greater.");
        }
        if (maxRetries < 0) {
            throw new ValidationException(
                    "Option '" + LookupOptions.MAX_RETRIES.key() + "' must be zero or greater.");
        }
        if (cacheType == LookupCacheType.PARTIAL) {
            // Fails planning, not the first lookup, on a cache setting Flink refuses.
            createPartialCache();
        }
        // Read only for FULL, the one cache type that consults the reload options.
        fullReload = cacheType == LookupCacheType.FULL ? FullCacheReloadConfig.from(config) : null;
    }

    /**
     * Reads and checks the lookup options.
     *
     * @param config the table's options
     * @return the lookup configuration
     * @throws ValidationException if a value is refused
     */
    public static DatastoreLookupConfig from(ReadableConfig config) {
        return new DatastoreLookupConfig(config);
    }

    /** Returns whether lookups read through the asynchronous API. */
    public boolean isAsync() {
        return async;
    }

    /** Returns the cache type. */
    public LookupCacheType getCacheType() {
        return cacheType;
    }

    /** Returns how many times a transient failure is read again after the first attempt. */
    public int getMaxRetries() {
        return maxRetries;
    }

    /** Creates the partial cache the options describe. */
    public DefaultLookupCache createPartialCache() {
        Configuration config = new Configuration();
        config.set(LookupOptions.CACHE_TYPE, cacheType);
        if (expireAfterAccess != null) {
            config.set(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS, expireAfterAccess);
        }
        if (expireAfterWrite != null) {
            config.set(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE, expireAfterWrite);
        }
        config.set(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY, cacheMissingKey);
        if (maxRows != null) {
            config.set(LookupOptions.PARTIAL_CACHE_MAX_ROWS, maxRows);
        }
        return DefaultLookupCache.fromConfig(config);
    }

    /**
     * Creates the full cache's reload trigger the options describe.
     *
     * @return a new reload trigger
     * @throws IllegalStateException if the cache type is not {@code FULL}
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

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof DatastoreLookupConfig)) {
            return false;
        }
        DatastoreLookupConfig that = (DatastoreLookupConfig) other;
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
