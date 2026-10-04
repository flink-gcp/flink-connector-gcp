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

package io.github.flink.gcp.connector.firestore.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/**
 * The {@code lookup.*} options of a {@code firestore} table, read and checked when the statement is
 * planned.
 *
 * <p>Flink's {@code NONE} and {@code PARTIAL} caches are accepted and Flink owns them; {@code FULL}
 * is refused, because a full cache is loaded by a scan, with a snapshot and a reload contract of
 * its own.
 */
@Internal
public final class FirestoreLookupConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final boolean async;
    private final LookupCacheType cacheType;
    private final int maxRetries;
    @Nullable private final Duration expireAfterAccess;
    @Nullable private final Duration expireAfterWrite;
    private final boolean cacheMissingKey;
    @Nullable private final Long maxRows;

    private FirestoreLookupConfig(ReadableConfig config) {
        async = config.get(FirestoreConnectorOptions.LOOKUP_ASYNC);
        cacheType = config.get(LookupOptions.CACHE_TYPE);
        maxRetries = config.get(LookupOptions.MAX_RETRIES);
        expireAfterAccess =
                config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS).orElse(null);
        expireAfterWrite =
                config.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE).orElse(null);
        cacheMissingKey = config.get(LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);
        maxRows = config.getOptional(LookupOptions.PARTIAL_CACHE_MAX_ROWS).orElse(null);
        if (cacheType == LookupCacheType.FULL) {
            throw new ValidationException(
                    "Option '"
                            + LookupOptions.CACHE_TYPE.key()
                            + "' does not support FULL for the Firestore table source; use"
                            + " PARTIAL or NONE.");
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
    }

    /**
     * Reads and checks the lookup options.
     *
     * @param config the table's options
     * @return the lookup configuration
     * @throws ValidationException if a value is refused
     */
    public static FirestoreLookupConfig from(ReadableConfig config) {
        return new FirestoreLookupConfig(config);
    }

    /** Returns whether lookups read through the asynchronous API. */
    public boolean isAsync() {
        return async;
    }

    /** Returns the cache type, {@code NONE} or {@code PARTIAL}. */
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

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FirestoreLookupConfig)) {
            return false;
        }
        FirestoreLookupConfig that = (FirestoreLookupConfig) other;
        return async == that.async
                && maxRetries == that.maxRetries
                && cacheMissingKey == that.cacheMissingKey
                && cacheType == that.cacheType
                && Objects.equals(expireAfterAccess, that.expireAfterAccess)
                && Objects.equals(expireAfterWrite, that.expireAfterWrite)
                && Objects.equals(maxRows, that.maxRows);
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
                maxRows);
    }
}
