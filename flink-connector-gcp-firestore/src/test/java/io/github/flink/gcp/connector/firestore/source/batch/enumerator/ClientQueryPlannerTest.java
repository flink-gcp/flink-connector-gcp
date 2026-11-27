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

package io.github.flink.gcp.connector.firestore.source.batch.enumerator;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.firestore.v1.Cursor;
import com.google.firestore.v1.PartitionQueryRequest;
import com.google.firestore.v1.PartitionQueryResponse;
import com.google.firestore.v1.Value;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** What the planner adds to a partitioning failure, and how it pages {@code PartitionQuery}. */
class ClientQueryPlannerTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "db");

    private static IOException failure(Status.Code code) {
        return new IOException(
                "Failed to partition the Firestore collection group scan.",
                new StatusRuntimeException(Status.fromCode(code).withDescription("refused")));
    }

    @Test
    void namesOnePartitionWhenTheServiceRefusesAsAPrecondition() {
        IOException failure = failure(Status.Code.FAILED_PRECONDITION);

        IOException planned = ClientQueryPlanner.partitionFailure(failure, DATABASE, 4);

        assertThat(planned)
                .hasMessageContaining("into 4 partitions")
                .hasMessageContaining("partitionCount(1)")
                .hasMessageContaining("'scan.partition.max-partitions' = '1'")
                .hasCause(failure.getCause());
    }

    @Test
    void leavesAnyOtherFailureAsItIs() {
        IOException denied = failure(Status.Code.PERMISSION_DENIED);
        IOException interrupted = new IOException("Interrupted while reading Firestore.");

        assertThat(ClientQueryPlanner.partitionFailure(denied, DATABASE, 4)).isSameAs(denied);
        assertThat(ClientQueryPlanner.partitionFailure(interrupted, DATABASE, 4))
                .isSameAs(interrupted);
    }

    /** A closed planner keeps no reference to the credentials it was given. */
    @Test
    void closingThePlannerDropsItsCredentials() throws Exception {
        GoogleCredentials credentials = GoogleCredentials.create(new AccessToken("t", null));
        ClientQueryPlanner planner = new ClientQueryPlanner(null);
        planner.useCredentials(credentials);
        assertThat(planner.settings(DATABASE).build().getCredentials()).isSameAs(credentials);

        planner.close();

        // Not through settings(...).build(), which would look for Application Default Credentials.
        assertThat(planner.credentials()).isNull();
    }

    /** The documents root of the database the paging tests' cursors name. */
    private static final String ROOT = "projects/p/databases/(default)/documents";

    @Test
    void followsThePageTokenToTheLastPage() throws IOException {
        PartitionQueryRequest first =
                PartitionQueryRequest.newBuilder().setParent(ROOT).setPartitionCount(9).build();
        Iterator<PartitionQueryResponse> answers =
                List.of(page("t1", "c/b"), page("t2"), page("", "c/a", "c/c")).iterator();
        List<PartitionQueryRequest> sent = new ArrayList<>();

        List<Cursor> cursors =
                ClientQueryPlanner.cursors(
                        first,
                        request -> {
                            sent.add(request);
                            return answers.next();
                        });

        assertThat(sent)
                .extracting(PartitionQueryRequest::getPageToken)
                .containsExactly("", "t1", "t2");
        assertThat(sent)
                .allSatisfy(
                        request ->
                                assertThat(request.toBuilder().clearPageToken().build())
                                        .isEqualTo(first));
        assertThat(cursors)
                .extracting(ClientQueryPlannerTest::path)
                .containsExactly("c/b", "c/a", "c/c");
    }

    @Test
    void aFailedPageFailsThePaging() {
        IOException failure = new IOException("page failed");
        Iterator<PartitionQueryResponse> answers = List.of(page("t1", "c/a")).iterator();

        assertThatThrownBy(
                        () ->
                                ClientQueryPlanner.cursors(
                                        PartitionQueryRequest.getDefaultInstance(),
                                        request -> {
                                            if (!answers.hasNext()) {
                                                throw failure;
                                            }
                                            return answers.next();
                                        }))
                .isSameAs(failure);
    }

    private static PartitionQueryResponse page(String nextPageToken, String... documents) {
        PartitionQueryResponse.Builder page =
                PartitionQueryResponse.newBuilder().setNextPageToken(nextPageToken);
        for (String document : documents) {
            page.addPartitions(
                    Cursor.newBuilder()
                            .addValues(
                                    Value.newBuilder().setReferenceValue(ROOT + "/" + document)));
        }
        return page.build();
    }

    private static String path(Cursor cursor) {
        return cursor.getValues(0).getReferenceValue().substring(ROOT.length() + 1);
    }
}
