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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.LookupOptions.ReloadStrategy;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger;
import org.apache.flink.table.connector.source.lookup.cache.trigger.PeriodicCacheReloadTrigger.ScheduleMode;
import org.apache.flink.table.connector.source.lookup.cache.trigger.TimedCacheReloadTrigger;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.format.DateTimeParseException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FullCacheReloadConfigTest {

    private static FullCacheReloadConfig reload(String... entries) {
        Configuration config = new Configuration();
        for (int i = 0; i < entries.length; i += 2) {
            config.setString(entries[i], entries[i + 1]);
        }
        return FullCacheReloadConfig.from(config);
    }

    @Test
    void retainsThePeriodicReloadIntervalAndScheduleMode() {
        FullCacheReloadConfig reload =
                reload(
                        "lookup.full-cache.periodic-reload.interval",
                        "7 min",
                        "lookup.full-cache.periodic-reload.schedule-mode",
                        "fixed_rate");

        assertThat(reload.createTrigger()).isInstanceOf(PeriodicCacheReloadTrigger.class);
        Configuration mapped = reload.asConfiguration();
        assertThat(mapped.get(LookupOptions.FULL_CACHE_RELOAD_STRATEGY))
                .isEqualTo(ReloadStrategy.PERIODIC);
        assertThat(mapped.get(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL))
                .isEqualTo(Duration.ofMinutes(7));
        assertThat(mapped.get(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_SCHEDULE_MODE))
                .isEqualTo(ScheduleMode.FIXED_RATE);
    }

    @Test
    void retainsTheTimedReloadTimeAndDayInterval() {
        FullCacheReloadConfig reload =
                reload(
                        "lookup.full-cache.reload-strategy",
                        "timed",
                        "lookup.full-cache.timed-reload.iso-time",
                        "10:15:30+09:00",
                        "lookup.full-cache.timed-reload.interval-in-days",
                        "3");

        assertThat(reload.createTrigger()).isInstanceOf(TimedCacheReloadTrigger.class);
        Configuration mapped = reload.asConfiguration();
        assertThat(mapped.get(LookupOptions.FULL_CACHE_RELOAD_STRATEGY))
                .isEqualTo(ReloadStrategy.TIMED);
        assertThat(mapped.get(LookupOptions.FULL_CACHE_TIMED_RELOAD_ISO_TIME))
                .isEqualTo("10:15:30+09:00");
        assertThat(mapped.get(LookupOptions.FULL_CACHE_TIMED_RELOAD_INTERVAL_IN_DAYS)).isEqualTo(3);
    }

    @Test
    void rejectsAPeriodicReloadIntervalBelowOneMillisecondWhileTheStatementIsPlanned() {
        assertThatThrownBy(() -> reload("lookup.full-cache.periodic-reload.interval", "999 micros"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "lookup.full-cache.periodic-reload.interval must be at least 1"
                                + " millisecond");
        reload("lookup.full-cache.periodic-reload.interval", "1 ms");
    }

    @Test
    void ignoresASubMillisecondPeriodicIntervalUnderTheTimedStrategy() {
        // The timed trigger never reads the periodic interval, so its floor does not apply.
        reload(
                "lookup.full-cache.reload-strategy",
                "timed",
                "lookup.full-cache.timed-reload.iso-time",
                "10:15",
                "lookup.full-cache.periodic-reload.interval",
                "999 micros");
    }

    @Test
    void namesTheOptionWhenTheTimedReloadTimeIsNotAnIsoTime() {
        assertThatThrownBy(
                        () ->
                                reload(
                                        "lookup.full-cache.reload-strategy",
                                        "timed",
                                        "lookup.full-cache.timed-reload.iso-time",
                                        "25:99"))
                .isInstanceOf(ValidationException.class)
                .hasMessage(
                        "Option 'lookup.full-cache.timed-reload.iso-time' must be an ISO-8601 time"
                                + " such as 10:15 or 10:15:30+09:00, but was '25:99'.")
                .hasCauseInstanceOf(DateTimeParseException.class);
    }

    @Test
    void letsFlinksTriggerRefuseAMissingPeriodicIntervalWhileTheStatementIsPlanned() {
        assertThatThrownBy(() -> reload())
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(ValidationException.class)
                .hasMessageContaining(LookupOptions.FULL_CACHE_PERIODIC_RELOAD_INTERVAL.key());
    }

    @Test
    void comparesByEveryReloadOption() {
        String[] base = {
            "lookup.full-cache.periodic-reload.interval", "1 h",
            "lookup.full-cache.timed-reload.iso-time", "10:15",
        };
        assertThat(reload(base)).isEqualTo(reload(base)).hasSameHashCodeAs(reload(base));
        assertThat(reload(base))
                .isNotEqualTo(
                        reload(
                                "lookup.full-cache.periodic-reload.interval",
                                "2 h",
                                "lookup.full-cache.timed-reload.iso-time",
                                "10:15"))
                .isNotEqualTo(
                        reload(
                                "lookup.full-cache.periodic-reload.interval",
                                "1 h",
                                "lookup.full-cache.timed-reload.iso-time",
                                "10:16"))
                .isNotEqualTo(
                        reload(
                                "lookup.full-cache.periodic-reload.interval",
                                "1 h",
                                "lookup.full-cache.timed-reload.iso-time",
                                "10:15",
                                "lookup.full-cache.periodic-reload.schedule-mode",
                                "fixed_rate"))
                .isNotEqualTo(
                        reload(
                                "lookup.full-cache.periodic-reload.interval",
                                "1 h",
                                "lookup.full-cache.timed-reload.iso-time",
                                "10:15",
                                "lookup.full-cache.timed-reload.interval-in-days",
                                "2"))
                .isNotEqualTo(
                        reload(
                                "lookup.full-cache.reload-strategy",
                                "timed",
                                "lookup.full-cache.periodic-reload.interval",
                                "1 h",
                                "lookup.full-cache.timed-reload.iso-time",
                                "10:15"));
    }

    @Test
    void survivesJavaSerialization() throws Exception {
        FullCacheReloadConfig reload =
                reload(
                        "lookup.full-cache.reload-strategy",
                        "timed",
                        "lookup.full-cache.timed-reload.iso-time",
                        "10:15",
                        "lookup.full-cache.timed-reload.interval-in-days",
                        "2");

        FullCacheReloadConfig copy =
                InstantiationUtil.clone(reload, FullCacheReloadConfig.class.getClassLoader());

        assertThat(copy).isEqualTo(reload);
        assertThat(copy.createTrigger()).isInstanceOf(TimedCacheReloadTrigger.class);
    }
}
