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

package io.github.flink.gcp.connector.bigquery.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;

import io.github.flink.gcp.connector.base.table.FullCacheReloadConfig;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * The {@code lookup.*} options of a {@code bigquery} table, read and checked when the statement is
 * planned.
 *
 * <p>A lookup join is served only by Flink's {@code FULL} cache, loaded by a read of the table; the
 * source refuses the other cache types when it plans a lookup, not here, because the same table may
 * be scanned under any {@code lookup.cache} value. The {@code lookup.full-cache.*} options are read
 * only for {@code FULL}, through {@link FullCacheReloadConfig}.
 */
@Internal
public final class BigQueryLookupConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private final LookupCacheType cacheType;
    @Nullable private final FullCacheReloadConfig fullReload;

    private BigQueryLookupConfig(ReadableConfig config) {
        cacheType = config.get(LookupOptions.CACHE_TYPE);
        // Read only for FULL, the one cache type that consults the reload options.
        fullReload = cacheType == LookupCacheType.FULL ? FullCacheReloadConfig.from(config) : null;
    }

    /**
     * Reads and checks the lookup options.
     *
     * @param config the table's options
     * @return the lookup configuration
     * @throws org.apache.flink.table.api.ValidationException if a full-cache periodic interval is
     *     shorter than one millisecond or too long to express in milliseconds, or a timed reload
     *     time is not an ISO-8601 time
     * @throws IllegalArgumentException if Flink's reload trigger refuses the options, such as a
     *     missing periodic interval
     */
    public static BigQueryLookupConfig from(ReadableConfig config) {
        return new BigQueryLookupConfig(config);
    }

    /** Returns the cache type. */
    public LookupCacheType getCacheType() {
        return cacheType;
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
        if (!(other instanceof BigQueryLookupConfig)) {
            return false;
        }
        BigQueryLookupConfig that = (BigQueryLookupConfig) other;
        return cacheType == that.cacheType && Objects.equals(fullReload, that.fullReload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cacheType, fullReload);
    }
}
