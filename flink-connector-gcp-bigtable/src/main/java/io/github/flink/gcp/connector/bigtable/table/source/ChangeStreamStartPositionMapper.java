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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.base.source.StartPosition;
import io.github.flink.gcp.connector.bigtable.table.ChangeStreamStartMode;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Optional;

/** Maps one mode/timestamp option pair onto the shared Change Streams start-position API. */
@Internal
public final class ChangeStreamStartPositionMapper {

    private ChangeStreamStartPositionMapper() {}

    /** Returns {@code null} when neither option is present, leaving the builder default intact. */
    @Nullable
    public static StartPosition map(
            ReadableConfig config,
            ConfigOption<ChangeStreamStartMode> modeOption,
            ConfigOption<Long> timestampOption) {
        Optional<ChangeStreamStartMode> mode = config.getOptional(modeOption);
        Optional<Long> timestamp = config.getOptional(timestampOption);
        if (!mode.isPresent()) {
            if (timestamp.isPresent()) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' was set without '%s', which is the option that would"
                                        + " use it. Set '%s' = '%s' to start from that instant.",
                                timestampOption.key(),
                                modeOption.key(),
                                modeOption.key(),
                                ChangeStreamStartMode.TIMESTAMP));
            }
            return null;
        }
        if (mode.get() == ChangeStreamStartMode.TIMESTAMP) {
            if (!timestamp.isPresent()) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' = '%s' requires option '%s'.",
                                modeOption.key(), mode.get(), timestampOption.key()));
            }
            return StartPosition.at(Instant.ofEpochMilli(timestamp.get()));
        }
        if (timestamp.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' is only valid when '%s' = '%s'.",
                            timestampOption.key(),
                            modeOption.key(),
                            ChangeStreamStartMode.TIMESTAMP));
        }
        return mode.get() == ChangeStreamStartMode.EARLIEST
                ? StartPosition.earliest()
                : StartPosition.latest();
    }
}
