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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.MutationGroup;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.ConstraintViolationPolicy;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What status Spanner answers each shape of bad mutation with — the measurement the writer's
 * failure classification rests on.
 *
 * <p>This is the emulator, so it is a convenience rather than an authority: where the real service
 * disagrees, the real service decides, and the gated real-GCP suite is where that is checked. What
 * the test does buy is that the classification is written against observed statuses rather than
 * guessed ones, and that a change in either the emulator or the classifier shows up here.
 *
 * <p>The shapes that matter to the decision are the ones that look data-shaped but are not routed:
 * a {@code NOT NULL} violation and a length overflow both answer {@code FAILED_PRECONDITION}, which
 * gRPC defines as the system not being in the required state — the same status Spanner uses for
 * conditions that clear on their own. Routing it would let a dropping policy discard good records
 * during an outage, so it fails the job instead.
 */
class SpannerRejectionITCase extends AbstractSpannerEmulatorITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerRejectionITCase.class);

    @Test
    void measuresWhatEachRejectionAnswersWith() throws Exception {
        SpannerDatabase database = rejectionDatabase();
        client(database).write(List.of(row("seeded", "unique-a", 1L)));

        Refusal duplicateKey = refusalFor(database, insert("seeded", "unique-b", 1L));
        Refusal duplicateIndexKey = refusalFor(database, insert("other", "unique-a", 1L));
        Refusal nullInNotNull =
                refusalFor(
                        database,
                        Mutation.newInsertOrUpdateBuilder("orders")
                                .set("id")
                                .to("x")
                                .set("uniq")
                                .to("unique-x")
                                .set("amount")
                                .to((Long) null)
                                .build());
        Refusal tooLong = refusalFor(database, row("y", "y".repeat(100), 1L));
        Refusal unknownColumn =
                refusalFor(
                        database,
                        Mutation.newInsertOrUpdateBuilder("orders")
                                .set("id")
                                .to("z")
                                .set("nope")
                                .to("z")
                                .build());
        Refusal unknownTable =
                refusalFor(
                        database,
                        Mutation.newInsertOrUpdateBuilder("nope").set("id").to("z").build());
        Refusal updateOfAMissingRow =
                refusalFor(
                        database,
                        Mutation.newUpdateBuilder("orders")
                                .set("id")
                                .to("gone")
                                .set("amount")
                                .to(1L)
                                .build());
        Refusal deleteOfAMissingRow =
                refusalFor(database, Mutation.delete("orders", Key.of("gone")));
        Refusal checkConstraint =
                refusalFor(
                        database,
                        Mutation.newInsertOrUpdateBuilder("orders")
                                .set("id")
                                .to("neg")
                                .set("uniq")
                                .to("unique-neg")
                                .set("amount")
                                .to(-1L)
                                .build());
        Refusal foreignKey =
                refusalFor(
                        database,
                        Mutation.newInsertOrUpdateBuilder("order_lines")
                                .set("line_id")
                                .to("l1")
                                .set("order_id")
                                .to("no-such-order")
                                .build());

        LOG.info(
                "Spanner emulator rejection statuses:"
                        + "\n  duplicate primary key : {}"
                        + "\n  duplicate index key   : {}"
                        + "\n  NULL in NOT NULL      : {}"
                        + "\n  value over max length : {}"
                        + "\n  unknown column        : {}"
                        + "\n  unknown table         : {}"
                        + "\n  update of a gone row  : {}"
                        + "\n  delete of a gone row  : {}"
                        + "\n  check constraint      : {}"
                        + "\n  foreign key           : {}",
                duplicateKey,
                duplicateIndexKey,
                nullInNotNull,
                tooLong,
                unknownColumn,
                unknownTable,
                updateOfAMissingRow,
                deleteOfAMissingRow,
                checkConstraint,
                foreignKey);

        // Routed: a replayed insert is the expected cost of at-least-once delivery, and the row it
        // describes is in the database either way.
        assertThat(duplicateKey.code).isEqualTo(StatusCode.Code.ALREADY_EXISTS);
        assertThat(duplicateIndexKey.code).isEqualTo(StatusCode.Code.ALREADY_EXISTS);
        assertThat(
                        SpannerErrorClassifier.classify(
                                duplicateKey.code, ConstraintViolationPolicy.FAIL_JOB))
                .isEqualTo(SpannerErrorClassifier.Kind.ROW_LEVEL);

        // Not routed, and this is the decision worth pinning: both look like bad data, and both
        // arrive under a status that also means "not right now".
        assertThat(nullInNotNull.code).isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
        assertThat(tooLong.code).isEqualTo(StatusCode.Code.FAILED_PRECONDITION);
        assertThat(
                        SpannerErrorClassifier.classify(
                                nullInNotNull.code, ConstraintViolationPolicy.FAIL_JOB))
                .isEqualTo(SpannerErrorClassifier.Kind.FATAL);

        // Configuration, not data: these fail every record alike, and this sink creates nothing.
        assertThat(unknownColumn.code).isEqualTo(StatusCode.Code.NOT_FOUND);
        assertThat(unknownTable.code).isEqualTo(StatusCode.Code.NOT_FOUND);

        // The same status for a case that is neither: an `update` whose row is gone. It is data,
        // and it still fails the job, because NOT_FOUND does not say whether the table or the row
        // is what is missing — and being wrong the other way would drop records over a typo in a
        // table name. Pinned here because the docs promise this behaviour to a user choosing an
        // operation.
        assertThat(updateOfAMissingRow.code).isEqualTo(StatusCode.Code.NOT_FOUND);
        assertThat(
                        SpannerErrorClassifier.classify(
                                updateOfAMissingRow.code, ConstraintViolationPolicy.FAIL_JOB))
                .isEqualTo(SpannerErrorClassifier.Kind.FATAL);
        // A delete of a row that is not there is simply applied, which is what makes a replayed
        // delete harmless.
        assertThat(deleteOfAMissingRow.code).isNull();
    }

    @Test
    void aRefusalNamesTheGroupItIsAboutRatherThanFailingTheRequest() throws Exception {
        SpannerDatabase database = rejectionDatabase();
        client(database).write(List.of(row("seeded", "unique-a", 1L)));

        Refusal refusal = refusalFor(database, insert("seeded", "unique-c", 1L));

        // Both halves matter. Without the code assertion this passes when the mutation is simply
        // applied, since an applied group is reported through the same callback.
        assertThat(refusal.code).isEqualTo(StatusCode.Code.ALREADY_EXISTS);
        // Per-group reporting is the whole reason this sink writes through batchWriteAtLeastOnce
        // rather than a plain commit, so it is asserted rather than assumed.
        assertThat(refusal.perGroup).isTrue();
    }

    // ---------------------------------------------------------------- helpers

    private static SpannerDatabase rejectionDatabase() throws Exception {
        return createDatabase(
                Dialect.GOOGLE_STANDARD_SQL,
                "CREATE TABLE orders (id STRING(64) NOT NULL, uniq STRING(64),"
                        + " amount INT64 NOT NULL) PRIMARY KEY (id)",
                "CREATE UNIQUE INDEX orders_by_uniq ON orders (uniq)",
                "ALTER TABLE orders ADD CONSTRAINT amount_positive CHECK (amount > 0)",
                "CREATE TABLE order_lines (line_id STRING(64) NOT NULL, order_id STRING(64),"
                        + " CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders (id))"
                        + " PRIMARY KEY (line_id)");
    }

    private Refusal refusalFor(SpannerDatabase database, Mutation mutation) throws Exception {
        try (SpannerDatabaseAccess access =
                new DefaultSpannerDatabaseAccessFactory(
                                database,
                                SpannerWriterOptions.defaults(),
                                EmulatorEndpoint.parse(emulatorEndpoint()))
                        .create()) {
            Refusal refusal = new Refusal();
            try {
                access.batchWrite(
                        List.of(MutationGroup.of(mutation)),
                        (groupIndex, status) -> {
                            refusal.perGroup = true;
                            refusal.code =
                                    SpannerErrorClassifier.fromCanonicalCode(status.getCode());
                        });
            } catch (RuntimeException e) {
                refusal.perGroup = false;
                refusal.code = SpannerErrorClassifier.statusCode(e);
            }
            return refusal;
        }
    }

    private static Mutation row(String id, String uniq, long amount) {
        return Mutation.newInsertOrUpdateBuilder("orders")
                .set("id")
                .to(id)
                .set("uniq")
                .to(uniq)
                .set("amount")
                .to(amount)
                .build();
    }

    private static Mutation insert(String id, String uniq, long amount) {
        return Mutation.newInsertBuilder("orders")
                .set("id")
                .to(id)
                .set("uniq")
                .to(uniq)
                .set("amount")
                .to(amount)
                .build();
    }

    /** What the service answered one bad mutation with, and whether it said so per group. */
    private static final class Refusal {

        @Nullable private StatusCode.Code code;
        private boolean perGroup;

        @Override
        public String toString() {
            return (code == null ? "OK" : code.toString()) + (perGroup ? " (per group)" : "");
        }
    }
}
