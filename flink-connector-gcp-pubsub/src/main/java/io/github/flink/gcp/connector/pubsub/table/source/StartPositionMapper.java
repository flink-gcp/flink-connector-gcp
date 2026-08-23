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

package io.github.flink.gcp.connector.pubsub.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.pubsub.source.PubSubStartPosition;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;

import javax.annotation.Nullable;

import java.time.Instant;
import java.util.Optional;

/**
 * Builds a {@link PubSubStartPosition} from the table options.
 *
 * <p>A start position is a mode plus, for one mode, an instant, which is why it is two options
 * rather than one. {@link PubSubStartPosition#of(PubSubStartPosition.Mode, Instant)} is the entry
 * point its javadoc names for exactly this, and it already rejects a missing instant under {@code
 * timestamp} and a present one under every other mode — so those two messages are the DataStream
 * API's, not this layer's.
 *
 * <p>The one rule that has to live here is a timestamp given with no mode. {@code of} never runs in
 * that case, so nothing downstream would notice, and the option would be silently ignored.
 */
@Internal
public final class StartPositionMapper {

    private StartPositionMapper() {}

    /**
     * Maps the table options onto a start position.
     *
     * @param config the table options
     * @return the start position, or {@code null} when the configuration names no mode, leaving the
     *     source builder's own default
     */
    @Nullable
    public static PubSubStartPosition map(ReadableConfig config) {
        Optional<Instant> timestamp =
                config.getOptional(PubSubConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)
                        .map(Instant::ofEpochMilli);
        Optional<PubSubStartPosition.Mode> mode =
                config.getOptional(PubSubConnectorOptions.SCAN_STARTUP_MODE);
        if (!mode.isPresent()) {
            if (timestamp.isPresent()) {
                throw new ValidationException(
                        String.format(
                                "Option '%s' was set without '%s', which is the option that would"
                                        + " use it. Set '%s' = '%s' to start from that instant.",
                                PubSubConnectorOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key(),
                                PubSubConnectorOptions.SCAN_STARTUP_MODE.key(),
                                PubSubConnectorOptions.SCAN_STARTUP_MODE.key(),
                                PubSubStartPosition.Mode.TIMESTAMP));
            }
            return null;
        }
        return PubSubStartPosition.of(mode.get(), timestamp.orElse(null));
    }
}
