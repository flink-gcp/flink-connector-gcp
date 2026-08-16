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

package io.github.flink.gcp.connector.pubsub.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import io.github.flink.gcp.connector.base.options.OptionChecks;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Settings the sink applies when it creates a topic that does not exist.
 *
 * <p>Unlike the source's {@code SubscriptionCreateOptions}, supplying these options is <em>not</em>
 * what authorises creation — {@link CreateDisposition} is, because a topic (unlike a subscription)
 * can meaningfully be created with defaults. The options are purely additive: without them, {@link
 * CreateDisposition#CREATE_IF_NEEDED} creates the topic with every field at its service default.
 * Combining them with {@link CreateDisposition#CREATE_NEVER} is rejected by {@link
 * PubSubSinkBuilder#build()}, since they would configure a topic the sink never creates.
 *
 * <p>One options object applies to <em>every</em> topic the sink creates. With dynamic destinations
 * (a {@link DestinationResolver}) each missing topic is created with these same settings; there is
 * no per-topic map, because unlike a subscription's topic binding, nothing in the settings ties
 * them to one topic.
 *
 * <p>Options only affect creation. A topic that already exists is used exactly as it is configured,
 * and these settings are neither applied to it nor compared against it.
 *
 * <p>Every knob is optional and unset means absent, leaving Pub/Sub's own default. Values are
 * validated here only where the failure would otherwise be silent or obscure; documented service
 * ranges (a message retention of 10 minutes to 31 days, for example) are left to Pub/Sub, whose
 * rejection already names the field and the limit. An all-unset object is allowed and equivalent to
 * supplying no options at all.
 */
@PublicEvolving
public final class TopicCreateOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    @Nullable private final Duration messageRetention;
    @Nullable private final String kmsKeyName;
    @Nullable private final List<String> allowedPersistenceRegions;
    private final boolean enforceInTransit;

    private TopicCreateOptions(Builder builder) {
        this.messageRetention = builder.messageRetention;
        this.kmsKeyName = builder.kmsKeyName;
        this.allowedPersistenceRegions = builder.allowedPersistenceRegions;
        this.enforceInTransit = builder.enforceInTransit;
    }

    /** Returns a builder. Every knob is optional. */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns how long the topic retains published messages, or {@code null} for the Pub/Sub
     * default (no topic-level retention).
     */
    @Nullable
    public Duration getMessageRetention() {
        return messageRetention;
    }

    /**
     * Returns the Cloud KMS key the topic encrypts messages with, or {@code null} for
     * Google-managed encryption.
     */
    @Nullable
    public String getKmsKeyName() {
        return kmsKeyName;
    }

    /**
     * Returns the regions messages published to the topic may be persisted in, or {@code null} when
     * the organization policy decides.
     */
    @Nullable
    public List<String> getAllowedPersistenceRegions() {
        return allowedPersistenceRegions;
    }

    /** Returns whether publishes from outside the allowed regions are rejected in transit. */
    public boolean isEnforceInTransit() {
        return enforceInTransit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicCreateOptions that = (TopicCreateOptions) o;
        return enforceInTransit == that.enforceInTransit
                && Objects.equals(messageRetention, that.messageRetention)
                && Objects.equals(kmsKeyName, that.kmsKeyName)
                && Objects.equals(allowedPersistenceRegions, that.allowedPersistenceRegions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                messageRetention, kmsKeyName, allowedPersistenceRegions, enforceInTransit);
    }

    @Override
    public String toString() {
        return "TopicCreateOptions{messageRetention="
                + messageRetention
                + ", kmsKeyName="
                + kmsKeyName
                + ", allowedPersistenceRegions="
                + allowedPersistenceRegions
                + ", enforceInTransit="
                + enforceInTransit
                + "}";
    }

    /** Builder for {@link TopicCreateOptions}. */
    @PublicEvolving
    public static final class Builder {

        @Nullable private Duration messageRetention;
        @Nullable private String kmsKeyName;
        @Nullable private List<String> allowedPersistenceRegions;
        private boolean enforceInTransit;

        private Builder() {}

        /**
         * Sets how long the topic retains published messages, whether or not they were
         * acknowledged. Defaults to the Pub/Sub default: no topic-level retention, so a message is
         * kept only as long as some subscription's own retention covers it. Topic retention is what
         * lets a subscription created <em>later</em> — or a backwards seek — reach messages
         * published before it existed or already acknowledged.
         *
         * @param messageRetention the retention duration
         * @return this builder
         */
        public Builder messageRetention(Duration messageRetention) {
            OptionChecks.checkPositive(messageRetention, "messageRetention");
            this.messageRetention = messageRetention;
            return this;
        }

        /**
         * Encrypts messages published to the topic with the given Cloud KMS key (customer-managed
         * encryption) instead of Google-managed encryption. The key must already exist, and the
         * Pub/Sub service account needs {@code cloudkms.cryptoKeyEncrypterDecrypter} on it —
         * without that grant, publishes to the created topic fail.
         *
         * @param kmsKeyName the full key resource name, {@code
         *     projects/P/locations/L/keyRings/R/cryptoKeys/K}
         * @return this builder
         */
        public Builder kmsKeyName(String kmsKeyName) {
            Preconditions.checkNotNull(kmsKeyName, "kmsKeyName must not be null");
            Preconditions.checkArgument(
                    !StringUtils.isNullOrWhitespaceOnly(kmsKeyName),
                    "kmsKeyName must not be blank");
            this.kmsKeyName = kmsKeyName;
            return this;
        }

        /**
         * Restricts which regions messages published to the topic may be persisted in (the topic's
         * message storage policy). Defaults to whatever the project's organization policy allows.
         *
         * @param allowedPersistenceRegions the allowed Cloud regions, for example {@code
         *     ["europe-west1", "europe-west4"]}
         * @return this builder
         */
        public Builder allowedPersistenceRegions(List<String> allowedPersistenceRegions) {
            Preconditions.checkNotNull(
                    allowedPersistenceRegions, "allowedPersistenceRegions must not be null");
            Preconditions.checkArgument(
                    !allowedPersistenceRegions.isEmpty(),
                    "allowedPersistenceRegions must not be empty");
            for (String region : allowedPersistenceRegions) {
                Preconditions.checkArgument(
                        !StringUtils.isNullOrWhitespaceOnly(region),
                        "allowedPersistenceRegions must not contain a blank region, but was %s",
                        allowedPersistenceRegions);
            }
            this.allowedPersistenceRegions =
                    Collections.unmodifiableList(new ArrayList<>(allowedPersistenceRegions));
            return this;
        }

        /**
         * Rejects publishes travelling through regions outside the allowed persistence regions,
         * instead of only restricting where messages are stored. Requires {@link
         * #allowedPersistenceRegions(List)}, which {@link #build()} enforces.
         *
         * @param enforceInTransit whether to enforce the storage policy in transit
         * @return this builder
         */
        public Builder enforceInTransit(boolean enforceInTransit) {
            this.enforceInTransit = enforceInTransit;
            return this;
        }

        /**
         * Builds the options.
         *
         * @return the options
         */
        public TopicCreateOptions build() {
            Preconditions.checkState(
                    !enforceInTransit || allowedPersistenceRegions != null,
                    "enforceInTransit(true) requires allowedPersistenceRegions(...): there are no"
                            + " regions to enforce without a storage policy.");
            return new TopicCreateOptions(this);
        }
    }
}
