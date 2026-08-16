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

package io.github.flink.gcp.connector.pubsub.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.pubsub.sink.CreateDisposition;
import io.github.flink.gcp.connector.pubsub.sink.TopicCreateOptions;
import io.github.flink.gcp.connector.pubsub.table.PubSubConnectorOptions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds {@link TopicCreateOptions} from the table options.
 *
 * <p>Under the same contract as {@code PublisherOptionsMapper}: every knob is applied with {@code
 * getOptional(...).ifPresent(...)}, no default is introduced, and value validation is left to the
 * builder so a SQL user gets the message a DataStream user gets. The two rules owned here — the
 * disposition cross-check and the storage-policy pair — are owned because their messages must name
 * option keys rather than builder methods.
 *
 * <p><b>Unlike the source's {@code scan.auto-create.*}, these options do not authorize creation</b>
 * — {@code sink.create-disposition} does, and it defaults to {@code create-if-needed}, so the
 * settings alone are enough to configure the topics an unconfigured table already creates. What is
 * rejected is combining them with an explicit {@code create-never}, which never creates a topic
 * they could apply to.
 *
 * <p>{@code enforce-in-transit} without {@code allowed-regions} is rejected here <em>and</em> by
 * the builder's {@code build()} — unlike the source mapper's expiration pair the builder does have
 * its own exception, but it names builder methods, so the mapper still owns saying it in option
 * keys.
 */
@Internal
public final class TopicCreateOptionsMapper {

    private static final List<ConfigOption<?>> CREATE_SETTINGS =
            Arrays.asList(
                    PubSubConnectorOptions.SINK_AUTO_CREATE_MESSAGE_RETENTION,
                    PubSubConnectorOptions.SINK_AUTO_CREATE_KMS_KEY_NAME,
                    PubSubConnectorOptions.SINK_AUTO_CREATE_STORAGE_POLICY_ALLOWED_REGIONS,
                    PubSubConnectorOptions.SINK_AUTO_CREATE_STORAGE_POLICY_ENFORCE_IN_TRANSIT);

    private TopicCreateOptionsMapper() {}

    /**
     * Maps the table options onto the settings a missing topic is created with.
     *
     * @param config the table options
     * @return the creation settings, or {@code null} when no {@code sink.auto-create.*} option is
     *     set, which leaves a created topic on service defaults
     */
    @Nullable
    public static TopicCreateOptions map(ReadableConfig config) {
        List<String> set = new ArrayList<>();
        for (ConfigOption<?> option : CREATE_SETTINGS) {
            if (config.getOptional(option).isPresent()) {
                set.add(option.key());
            }
        }
        if (set.isEmpty()) {
            return null;
        }
        if (config.getOptional(PubSubConnectorOptions.SINK_CREATE_DISPOSITION).orElse(null)
                == CreateDisposition.CREATE_NEVER) {
            throw new ValidationException(
                    String.format(
                            "Options %s configure a topic this table never creates, because '%s' is"
                                    + " 'create-never'. Remove the options or use"
                                    + " 'create-if-needed'.",
                            set, PubSubConnectorOptions.SINK_CREATE_DISPOSITION.key()));
        }

        if (config.getOptional(
                                PubSubConnectorOptions
                                        .SINK_AUTO_CREATE_STORAGE_POLICY_ENFORCE_IN_TRANSIT)
                        .orElse(false)
                && !config.getOptional(
                                PubSubConnectorOptions
                                        .SINK_AUTO_CREATE_STORAGE_POLICY_ALLOWED_REGIONS)
                        .isPresent()) {
            throw new ValidationException(
                    String.format(
                            "Option '%s' = 'true' requires '%s': there are no regions to enforce"
                                    + " without a storage policy.",
                            PubSubConnectorOptions
                                    .SINK_AUTO_CREATE_STORAGE_POLICY_ENFORCE_IN_TRANSIT
                                    .key(),
                            PubSubConnectorOptions.SINK_AUTO_CREATE_STORAGE_POLICY_ALLOWED_REGIONS
                                    .key()));
        }

        TopicCreateOptions.Builder builder = TopicCreateOptions.builder();
        config.getOptional(PubSubConnectorOptions.SINK_AUTO_CREATE_MESSAGE_RETENTION)
                .ifPresent(builder::messageRetention);
        config.getOptional(PubSubConnectorOptions.SINK_AUTO_CREATE_KMS_KEY_NAME)
                .ifPresent(builder::kmsKeyName);
        config.getOptional(PubSubConnectorOptions.SINK_AUTO_CREATE_STORAGE_POLICY_ALLOWED_REGIONS)
                .ifPresent(builder::allowedPersistenceRegions);
        config.getOptional(
                        PubSubConnectorOptions.SINK_AUTO_CREATE_STORAGE_POLICY_ENFORCE_IN_TRANSIT)
                .ifPresent(builder::enforceInTransit);
        return builder.build();
    }
}
