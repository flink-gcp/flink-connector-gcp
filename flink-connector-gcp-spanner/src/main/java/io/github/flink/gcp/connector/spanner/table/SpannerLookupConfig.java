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

package io.github.flink.gcp.connector.spanner.table;

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
        if (cacheType == LookupCacheType.FULL) {
            throw new ValidationException(
                    "'lookup.cache' does not support FULL for the Spanner table source.");
        }
        if (maxRetries < 0) {
            throw new ValidationException("'lookup.max-retries' must be zero or greater.");
        }
        if (cacheType == LookupCacheType.PARTIAL) {
            createPartialCache();
        }
    }

    public static SpannerLookupConfig from(ReadableConfig config) {
        return new SpannerLookupConfig(config);
    }

    public boolean isAsync() {
        return async;
    }

    public LookupCacheType getCacheType() {
        return cacheType;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public DefaultLookupCache createPartialCache() {
        Configuration config = new Configuration();
        config.set(LookupOptions.CACHE_TYPE, cacheType);
        config.set(LookupOptions.MAX_RETRIES, maxRetries);
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
