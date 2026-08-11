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

package io.github.flink.gcp.connector.bigtable.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.TimedCacheReloadTrigger;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableLookupOptionsTest {

    private static BigtableLookupConfig options(String... entries) {
        Configuration config = new Configuration();
        for (int i = 0; i < entries.length; i += 2) {
            config.setString(entries[i], entries[i + 1]);
        }
        return BigtableLookupConfig.from(config);
    }

    @Test
    void defaultsToSynchronousUncachedLookupsAndThreeRetries() {
        BigtableLookupConfig options = options();

        assertThat(options.isAsync()).isFalse();
        assertThat(options.getCacheType()).isEqualTo(LookupCacheType.NONE);
        assertThat(options.getMaxRetries()).isEqualTo(3);
    }

    @Test
    void buildsFlinksDefaultPartialCacheFromTheStandardKeys() {
        BigtableLookupConfig options =
                options(
                        "lookup.cache",
                        "partial",
                        "lookup.partial-cache.expire-after-write",
                        "5 min",
                        "lookup.partial-cache.max-rows",
                        "100",
                        "lookup.partial-cache.cache-missing-key",
                        "false");

        assertThat(options.createPartialCache())
                .isEqualTo(
                        DefaultLookupCache.newBuilder()
                                .expireAfterWrite(Duration.ofMinutes(5))
                                .maximumSize(100)
                                .cacheMissingKey(false)
                                .build());
    }

    @Test
    void buildsBothStandardFullCacheReloadTriggers() {
        assertThat(
                        options(
                                        "lookup.cache",
                                        "full",
                                        "lookup.full-cache.periodic-reload.interval",
                                        "1 min")
                                .createFullReloadTrigger())
                .isInstanceOf(PeriodicCacheReloadTrigger.class);
        assertThat(
                        options(
                                        "lookup.cache",
                                        "full",
                                        "lookup.full-cache.reload-strategy",
                                        "timed",
                                        "lookup.full-cache.timed-reload.iso-time",
                                        "10:15Z")
                                .createFullReloadTrigger())
                .isInstanceOf(TimedCacheReloadTrigger.class);
    }

    @Test
    void rejectsPartialCachingWithoutAnEvictionBound() {
        assertThatThrownBy(() -> options("lookup.cache", "partial"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lookup.partial-cache.max-rows");
    }

    @Test
    void rejectsFullCachingWithAsyncLookup() {
        assertThatThrownBy(
                        () ->
                                options(
                                        "lookup.async",
                                        "true",
                                        "lookup.cache",
                                        "full",
                                        "lookup.full-cache.periodic-reload.interval",
                                        "1 min"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'lookup.async' cannot be true")
                .hasMessageContaining("'lookup.cache' is FULL");
    }

    @Test
    void rejectsANegativeRetryCount() {
        assertThatThrownBy(() -> options("lookup.max-retries", "-1"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("'lookup.max-retries' must be zero or greater");
    }
}
