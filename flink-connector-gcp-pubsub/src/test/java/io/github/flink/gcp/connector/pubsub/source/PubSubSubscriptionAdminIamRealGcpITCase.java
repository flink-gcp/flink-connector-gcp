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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import io.github.flink.gcp.connector.pubsub.sink.TopicDestination;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.PubSubSubscriptionAdmin;
import io.github.flink.gcp.connector.pubsub.source.subscriptions.SubscriptionAdmin;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@link PubSubSubscriptionAdmin} permission-denied branches, exercised with credentials that
 * genuinely lack the permissions — the emulator has no IAM at all, so these catch blocks can rot
 * silently anywhere else, and their message text is their entire value: an operator reads "your job
 * manager's credentials lack X" instead of an opaque gax stack trace. Running against the real
 * service also exercises gax's status-to-exception mapping, which a stubbed client would not (gax
 * decides which exception type a status maps to, and that mapping is where the fragility is).
 *
 * <p>The unauthorized identity is the {@code e2e-no-pubsub} service account provisioned in opentofu
 * with deliberately no Pub/Sub role, reached by impersonation (no key exists). The E2E workflow's
 * account carries the {@code roles/iam.serviceAccountTokenCreator} binding; running locally needs a
 * one-off self-grant first — see the documentation page's Testing section.
 */
@EnabledIfEnvironmentVariable(named = "PUBSUB_IT_PROJECT", matches = ".+")
class PubSubSubscriptionAdminIamRealGcpITCase extends AbstractPubSubRealGcpITCase {

    private static SubscriptionDestination existingSubscription;

    @BeforeAll
    static void createSubscriptionTheDeniedCallsTarget() {
        // Created with the harness's full-privilege credentials, so what the assertions below
        // exercise is denial on an existing resource, never absence.
        TopicDestination topic = createTopic("iam");
        existingSubscription = createSubscription(topic, "iam");
    }

    @Test
    void describeWithoutTheGetPermissionNamesThePermissionAndRole() {
        assertThatThrownBy(() -> deniedAdmin().describe(existingSubscription))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("pubsub.subscriptions.get")
                .hasMessageContaining("roles/pubsub.viewer")
                .hasCauseInstanceOf(PermissionDeniedException.class);
    }

    @Test
    void createWithoutTheCreatePermissionWrapsTheDenial() {
        TopicDestination topic = createTopic("iam-create");
        SubscriptionDestination denied =
                SubscriptionDestination.of(PROJECT, uniqueName("iam-create"));

        assertThatThrownBy(
                        () ->
                                deniedAdmin()
                                        .create(
                                                denied,
                                                SubscriptionCreateOptions.builder()
                                                        .topic(topic)
                                                        .build()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to create Pub/Sub subscription")
                .hasCauseInstanceOf(PermissionDeniedException.class);
        // The denied create must not have half-created anything.
        assertThat(describeSubscriptionExists(denied)).isFalse();
    }

    @Test
    void seekWithoutTheUpdatePermissionNamesThePermissionAndRole() {
        assertThatThrownBy(() -> deniedAdmin().seek(existingSubscription, Instant.now()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("pubsub.subscriptions.update")
                .hasMessageContaining("roles/pubsub.editor")
                .hasCauseInstanceOf(PermissionDeniedException.class);
    }

    /** The production admin, authenticating as the deliberately unauthorized identity. */
    private static SubscriptionAdmin deniedAdmin() throws IOException {
        ImpersonatedCredentials denied =
                ImpersonatedCredentials.newBuilder()
                        .setSourceCredentials(GoogleCredentials.getApplicationDefault())
                        .setTargetPrincipal("e2e-no-pubsub@" + PROJECT + ".iam.gserviceaccount.com")
                        .setScopes(List.of("https://www.googleapis.com/auth/cloud-platform"))
                        .build();
        return new PubSubSubscriptionAdmin(null, FixedCredentialsProvider.create(denied));
    }

    private static boolean describeSubscriptionExists(SubscriptionDestination subscription) {
        try {
            describeSubscription(subscription);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
