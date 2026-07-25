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

package io.github.flink.gcp.connector.pubsub.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.google.pubsub.v1.ProjectSubscriptionName;

import java.io.Serializable;
import java.util.Objects;

/**
 * A fully-qualified Pub/Sub subscription reference: project and subscription.
 *
 * <p>Instances are pure subscription <em>identity</em>: {@link #equals(Object)} and {@link
 * #hashCode()} are defined over exactly (project, subscription), so the class can serve as the key
 * of a split and of the reader's per-subscription subscriber map. Subscriber settings are
 * intentionally not part of this class — they are configured on the source — keeping subscription
 * identity stable.
 *
 * <p>Instances are immutable; the resource path and hash are precomputed.
 *
 * <p>Deliberately mirrors {@code sink.TopicDestination} (and the BigQuery module's {@code
 * TableDestination}) rather than sharing a base type: the three name different resources and only
 * coincide in shape. Extracting a shared destination-identity type is tracked with the other
 * cross-connector extractions in issue #61.
 */
@PublicEvolving
public final class SubscriptionDestination implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String project;
    private final String subscription;
    private final String subscriptionPath;
    private final int hash;

    private SubscriptionDestination(String project, String subscription) {
        this.project = project;
        this.subscription = subscription;
        this.subscriptionPath = ProjectSubscriptionName.format(project, subscription);
        this.hash = Objects.hash(project, subscription);
    }

    /**
     * Creates a {@link SubscriptionDestination}.
     *
     * @param project the Google Cloud project id
     * @param subscription the Pub/Sub subscription id
     * @return the destination
     */
    public static SubscriptionDestination of(String project, String subscription) {
        checkComponent(project, "project");
        checkComponent(subscription, "subscription");
        return new SubscriptionDestination(project, subscription);
    }

    private static void checkComponent(String value, String name) {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(value), "%s must not be blank", name);
        Preconditions.checkArgument(
                value.equals(value.trim()),
                "%s must not have leading or trailing whitespace: '%s'",
                name,
                value);
        Preconditions.checkArgument(
                value.indexOf('/') < 0, "%s must not contain '/': '%s'", name, value);
    }

    /** Returns the Google Cloud project id. */
    public String getProject() {
        return project;
    }

    /** Returns the Pub/Sub subscription id. */
    public String getSubscription() {
        return subscription;
    }

    /**
     * Returns the subscription path in the {@code projects/<p>/subscriptions/<s>} form used by the
     * Pub/Sub API.
     */
    public String toSubscriptionPath() {
        return subscriptionPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionDestination that = (SubscriptionDestination) o;
        return project.equals(that.project) && subscription.equals(that.subscription);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return project + "/" + subscription;
    }
}
