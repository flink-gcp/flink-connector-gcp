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

package io.github.flink.gcp.connector.spanner.sink;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FailedMutation}. */
class FailedMutationTest {

    private static final SpannerDatabase DATABASE = SpannerDatabase.of("p", "i", "d");

    @Test
    void carriesTheMutationTheServiceRefused() {
        Mutation mutation = Mutation.newInsertBuilder("Orders").set("OrderId").to(7L).build();

        FailedMutation failure = FailedMutation.of(DATABASE, mutation, "refused", null);

        assertThat(failure.getConnector()).isEqualTo("spanner");
        assertThat(failure.getDatabase()).isEqualTo(DATABASE);
        assertThat(failure.getMutation()).isSameAs(mutation);
        assertThat(failure.getTable()).isEqualTo("Orders");
        assertThat(failure.getErrorMessage()).isEqualTo("refused");
        assertThat(failure.getCause()).isNull();
        assertThat(failure.describeDestination())
                .isEqualTo("projects/p/instances/i/databases/d/tables/Orders");
    }

    @Test
    void thePayloadIsALosslessRoundTripOfTheMutation() throws Exception {
        // The whole reason the payload is Java-serialized rather than a protobuf: it has to come
        // back exactly, and the client library offers no other non-lossy encoding.
        String longValue = "x".repeat(500);
        Mutation mutation =
                Mutation.newInsertOrUpdateBuilder("Orders")
                        .set("OrderId")
                        .to(7L)
                        .set("Note")
                        .to(longValue)
                        .build();

        ByteString payload =
                FailedMutation.of(DATABASE, mutation, "refused", null).getPayloadBytes();

        assertThat(payload).isNotNull();
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(payload.toByteArray()))) {
            assertThat(in.readObject()).isEqualTo(mutation);
        }
    }

    @Test
    void theDebugRenderingIsStillLossy() {
        // The measured fact behind the choice above: Value.toString() cuts every string value at
        // 36 characters, so a payload built from it would look complete and not be. If this ever
        // starts passing the whole value through, the encoding decision is worth revisiting —
        // which is what this test is here to say.
        Mutation mutation =
                Mutation.newInsertBuilder("Orders").set("Note").to("x".repeat(500)).build();

        assertThat(mutation.toString()).contains("...").hasSizeLessThan(200);
    }

    @Test
    void carriesNoMutationWhenSerializationItselfFailed() {
        Exception cause = new IllegalStateException("boom");

        FailedMutation failure = FailedMutation.of(DATABASE, null, "could not serialize", cause);

        assertThat(failure.getMutation()).isNull();
        assertThat(failure.getTable()).isNull();
        // Null payload is the contract's marker for "no payload was ever produced", so it must
        // stay reachable only through this case.
        assertThat(failure.getPayloadBytes()).isNull();
        assertThat(failure.getCause()).isSameAs(cause);
        assertThat(failure.describeDestination()).isEqualTo("projects/p/instances/i/databases/d");
    }

    @Test
    void carriesADeleteMutationToo() {
        Mutation mutation = Mutation.delete("Orders", Key.of(7L));

        FailedMutation failure = FailedMutation.of(DATABASE, mutation, "refused", null);

        assertThat(failure.getTable()).isEqualTo("Orders");
        assertThat(failure.getPayloadBytes()).isNotNull();
    }

    @Test
    void rejectsMissingRequiredParts() {
        Mutation mutation = Mutation.delete("Orders", Key.of(7L));

        assertThatThrownBy(() -> FailedMutation.of(null, mutation, "refused", null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> FailedMutation.of(DATABASE, mutation, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rendersTheDatabaseTableAndMessage() {
        Mutation mutation = Mutation.delete("Orders", Key.of(7L));

        assertThat(FailedMutation.of(DATABASE, mutation, "refused", null).toString())
                .contains("projects/p/instances/i/databases/d")
                .contains("Orders")
                .contains("refused");
    }
}
