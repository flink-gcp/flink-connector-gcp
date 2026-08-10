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
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.MutationGroup;
import io.github.flink.gcp.connector.spanner.AbstractSpannerRealGcpITCase;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.ConstraintViolationPolicy;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What real Cloud Spanner answers each shape of bad mutation with — the measurement the writer's
 * failure classification rests on, taken from the service rather than from the emulator.
 *
 * <p>{@code SpannerRejectionITCase} asks the same questions of the emulator, and every row of the
 * table it produced is emulator evidence: a convenience, never an authority. This class is where
 * {@code docs/adr/0076}'s table is confirmed, including the two rows that decide what {@code
 * constraintViolationPolicy} has to cover, and where the per-group reporting the whole sink design
 * rests on ({@code docs/adr/0075}) is measured against the real RPC.
 *
 * <p>Two things only this class exercises, besides the statuses. The client is built without an
 * emulator endpoint, so this is the application-default-credentials construction path. And the
 * mutation-cell weights come out of the service's own {@code INFORMATION_SCHEMA} rather than the
 * emulator's, in both dialects.
 */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "SPANNER_IT_PROJECT", matches = ".+")
class SpannerRejectionRealGcpITCase extends AbstractSpannerRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerRejectionRealGcpITCase.class);

    private static final int MIB = 1024 * 1024;

    /** One megabyte a row, comfortably under Spanner's 2,621,440-character {@code STRING(MAX)}. */
    private static final int PAYLOAD_CHARS = MIB;

    private static SpannerDatabase rejectionDatabase;
    private static SpannerDatabase blobsDatabase;

    private static final Map<Dialect, SpannerDatabase> ordersDatabases =
            new EnumMap<>(Dialect.class);

    @BeforeAll
    static void createDatabases() throws Exception {
        // Once for the class, not once per test: on the service a database takes seconds to
        // create, and every test here either only reads the schema or writes rows of its own.
        rejectionDatabase =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE orders (id STRING(64) NOT NULL, uniq STRING(64),"
                                + " amount INT64 NOT NULL) PRIMARY KEY (id)",
                        "CREATE UNIQUE INDEX orders_by_uniq ON orders (uniq)",
                        "ALTER TABLE orders ADD CONSTRAINT amount_positive CHECK (amount > 0)",
                        "CREATE TABLE order_lines (line_id STRING(64) NOT NULL,"
                                + " order_id STRING(64), CONSTRAINT fk_order FOREIGN KEY"
                                + " (order_id) REFERENCES orders (id)) PRIMARY KEY (line_id)");
        client(rejectionDatabase).write(List.of(row("seeded", "unique-a", 1L)));

        blobsDatabase =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE blobs (id STRING(64) NOT NULL, payload STRING(MAX),"
                                + " raw BYTES(MAX)) PRIMARY KEY (id)");

        for (Dialect dialect : Dialect.values()) {
            ordersDatabases.put(dialect, ordersDatabase(dialect));
        }
    }

    @Test
    void measuresWhatEachRejectionAnswersWith() throws Exception {
        Refusal duplicateKey = refusalFor(insert("seeded", "unique-b", 1L));
        Refusal duplicateIndexKey = refusalFor(insert("other", "unique-a", 1L));
        Refusal nullInNotNull =
                refusalFor(
                        Mutation.newInsertOrUpdateBuilder("orders")
                                .set("id")
                                .to("x")
                                .set("uniq")
                                .to("unique-x")
                                .set("amount")
                                .to((Long) null)
                                .build());
        Refusal tooLong = refusalFor(row("y", "y".repeat(100), 1L));
        Refusal unknownColumn =
                refusalFor(
                        Mutation.newInsertOrUpdateBuilder("orders")
                                .set("id")
                                .to("z")
                                .set("nope")
                                .to("z")
                                .build());
        Refusal unknownTable =
                refusalFor(Mutation.newInsertOrUpdateBuilder("nope").set("id").to("z").build());
        Refusal updateOfAMissingRow =
                refusalFor(
                        Mutation.newUpdateBuilder("orders")
                                .set("id")
                                .to("gone")
                                .set("amount")
                                .to(1L)
                                .build());
        Refusal deleteOfAMissingRow = refusalFor(Mutation.delete("orders", Key.of("gone")));
        Refusal checkConstraint =
                refusalFor(
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
                        Mutation.newInsertOrUpdateBuilder("order_lines")
                                .set("line_id")
                                .to("l1")
                                .set("order_id")
                                .to("no-such-order")
                                .build());

        LOG.info(
                "Cloud Spanner rejection statuses:"
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

        // Not routed by default, and this is the decision worth pinning: both look like bad data,
        // and both arrive under a status that also means "not right now".
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
        // is what is missing.
        assertThat(updateOfAMissingRow.code).isEqualTo(StatusCode.Code.NOT_FOUND);
        // A delete of a row that is not there is simply applied, which is what makes a replayed
        // delete harmless.
        assertThat(deleteOfAMissingRow.code).isNull();
    }

    @Test
    void aConstraintViolationSpansTwoStatusesRatherThanOne() throws Exception {
        Refusal checkConstraint =
                refusalFor(
                        Mutation.newInsertOrUpdateBuilder("orders")
                                .set("id")
                                .to("neg-2")
                                .set("uniq")
                                .to("unique-neg-2")
                                .set("amount")
                                .to(-1L)
                                .build());
        Refusal foreignKey =
                refusalFor(
                        Mutation.newInsertOrUpdateBuilder("order_lines")
                                .set("line_id")
                                .to("l2")
                                .set("order_id")
                                .to("no-such-order")
                                .build());

        // The measurement docs/adr/0076 turns into a rule: constraintViolationPolicy has to cover
        // both statuses, because Spanner does not answer constraint violations with one. A policy
        // that covered only FAILED_PRECONDITION would silently fail the job on a CHECK violation
        // the user asked to route, and vice versa.
        assertThat(checkConstraint.code).isEqualTo(StatusCode.Code.OUT_OF_RANGE);
        assertThat(foreignKey.code).isEqualTo(StatusCode.Code.FAILED_PRECONDITION);

        for (Refusal violation : List.of(checkConstraint, foreignKey)) {
            assertThat(
                            SpannerErrorClassifier.classify(
                                    violation.code, ConstraintViolationPolicy.FAIL_JOB))
                    .isEqualTo(SpannerErrorClassifier.Kind.FATAL);
            assertThat(
                            SpannerErrorClassifier.classify(
                                    violation.code,
                                    ConstraintViolationPolicy.ROUTE_TO_FAILURE_HANDLER))
                    .isEqualTo(SpannerErrorClassifier.Kind.ROW_LEVEL);
        }
    }

    @Test
    void aRefusalNamesTheGroupItIsAboutRatherThanFailingTheRequest() throws Exception {
        Refusal refusal = refusalFor(insert("seeded", "unique-c", 1L));

        // Both halves matter. Without the code assertion this passes when the mutation is simply
        // applied, since an applied group is reported through the same callback.
        assertThat(refusal.code).isEqualTo(StatusCode.Code.ALREADY_EXISTS);
        // Per-group reporting is the whole reason this sink writes through batchWriteAtLeastOnce
        // rather than a plain commit. The emulator reports per group; that it is the service's
        // behaviour and not the emulator's convenience is what this asserts.
        assertThat(refusal.perGroup).isTrue();
    }

    @Test
    void aGoodMutationInTheSameRequestIsStillApplied() throws Exception {
        List<MutationGroup> groups =
                List.of(
                        MutationGroup.of(insert("seeded", "unique-d", 1L)),
                        MutationGroup.of(row("kept-1", "unique-kept-1", 1L)),
                        MutationGroup.of(row("kept-2", "unique-kept-2", 1L)));

        Map<Integer, String> outcomes = outcomesOf(groups);

        // Keyed by group index and holding "OK" rather than null for an applied group, so that a
        // service reporting only the *failures* could not satisfy this: with a null for "applied"
        // the two assertions below would pass on a callback that never fired for them, which is
        // the wrong reason for this test to be green.
        assertThat(outcomes).containsOnlyKeys(0, 1, 2);
        // One group refused, the other two applied, in one request — the property that makes
        // per-record failure routing possible at all.
        assertThat(outcomes.get(0)).isEqualTo(StatusCode.Code.ALREADY_EXISTS.name());
        assertThat(outcomes.get(1)).isEqualTo("OK");
        assertThat(outcomes.get(2)).isEqualTo("OK");
        assertThat(ids("WHERE id LIKE 'kept-%'")).containsExactly("kept-1", "kept-2");
    }

    @Test
    void measuresHowLargeABatchWriteRequestMayBe() throws Exception {
        // Issue #441. The documentation does not settle this: the batch-write page's one sentence
        // about size, read as a carve-out, puts this RPC on the 100 MiB commit row, and the
        // quotas page's "request size other than for commits" row, read literally, puts it on
        // 10 MiB. The quotas page has no batch-write row to break the tie, and the emulator is not
        // an authority about quota enforcement — so the service is the only place to ask.
        Outcome under = sendApproximately(8 * MIB);
        Outcome overTenMib = sendApproximately(12 * MIB);
        Outcome overHundredMib =
                overTenMib.accepted() ? sendApproximately(110 * MIB) : Outcome.notRun();

        LOG.info(
                "Cloud Spanner batch-write request size ceiling:"
                        + "\n   8 MiB : {}"
                        + "\n  12 MiB : {}"
                        + "\n 110 MiB : {}",
                under,
                overTenMib,
                overHundredMib);

        // The control arm. Without it a refusal below could be about anything — a malformed
        // payload, a lost session — rather than about size.
        assertThat(under.accepted())
                .as("a request well under either candidate ceiling must be accepted")
                .isTrue();
        // #441's answer, measured 2026-08-10: the 100 MiB reading holds and the 10 MiB one does
        // not, so MAX_BATCH_BYTES_LIMIT stays where it is. Should this ever start failing, the
        // ceiling moved down and that constant has to move with it.
        assertThat(overTenMib.accepted())
                .as(
                        "a request over 10 MiB is accepted, so that reading of the quota is not this RPC's")
                .isTrue();
        // And the ceiling that does exist, named exactly. The message rather than only the code,
        // because RESOURCE_EXHAUSTED alone would not distinguish a size refusal from an instance
        // at capacity — and the figure is the one MAX_BATCH_BYTES_LIMIT is set from.
        assertThat(overHundredMib.failure)
                .as("a request over 100 MiB must be refused")
                .isNotNull()
                .contains("104857600");
    }

    @Test
    void aBytesValueCostsFourThirdsOfItselfOnTheWire() throws Exception {
        // Why this is worth a request of its own: Spanner carries every value inside a
        // google.protobuf.Value, which has no bytes kind, so a BYTES column travels as a base64
        // string. MutationSizeEstimator counts BYTES by its raw length, so without this the
        // estimate for a BYTES-heavy batch reads a quarter low — enough for a job that raised
        // maxBatchBytes toward its ceiling to build a request the service always refuses, and
        // refuses under RESOURCE_EXHAUSTED, which this connector retries.
        int rawBytes = 80 * MIB;
        Outcome refused = sendRawBytes(rawBytes);

        LOG.info("A batch of {} raw BYTES: {}", rawBytes, refused);

        // 80 MiB of raw bytes is comfortably under the 100 MiB ceiling; base64 of it is not. That
        // the service refuses this at all is the measurement — a request sized by the raw length
        // would have been accepted.
        assertThat(refused.failure)
                .as("80 MiB of raw BYTES exceeds the ceiling once base64-encoded")
                .isNotNull()
                .contains("104857600");
        // And the reported size names the ratio rather than leaving it inferred.
        long reported = reportedRequestSize(refused.failure);
        assertThat(reported)
                .as("the wire size of %s raw bytes, base64-encoded", rawBytes)
                .isBetween(rawBytes * 4L / 3, rawBytes * 4L / 3 + MIB);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void readsTheSecondaryIndexCoverageOutOfTheRealSchema(Dialect dialect) throws Exception {
        CellWeights weights;
        try (SpannerDatabaseAccess access = access(ordersDatabases.get(dialect))) {
            weights = access.readCellWeights();
        }

        // One secondary index covers name, and the primary-key index is excluded — so a mutation
        // writing id and name costs 1 + 2 rather than 2. Emulator-verified in SpannerWriteITCase;
        // this is the same assertion against the service's own INFORMATION_SCHEMA, whose
        // dialect-branched shape docs/adr/0077 had only emulator evidence for.
        assertThat(weights.knows("orders")).isTrue();
        assertThat(
                        weights.weigh(
                                Mutation.newInsertBuilder("orders")
                                        .set("id")
                                        .to("a")
                                        .set("name")
                                        .to("a")
                                        .build()))
                .isEqualTo(3);
    }

    // ---------------------------------------------------------------- helpers

    private static SpannerDatabase ordersDatabase(Dialect dialect) throws Exception {
        if (dialect == Dialect.POSTGRESQL) {
            return createDatabase(
                    dialect,
                    "CREATE TABLE orders (id varchar(64) NOT NULL PRIMARY KEY, name varchar(64))",
                    "CREATE INDEX orders_by_name ON orders (name)");
        }
        return createDatabase(
                dialect,
                "CREATE TABLE orders (id STRING(64) NOT NULL, name STRING(64)) PRIMARY KEY (id)",
                "CREATE INDEX orders_by_name ON orders (name)");
    }

    /** Opens the production database access over application-default credentials. */
    private static SpannerDatabaseAccess access(SpannerDatabase database) throws IOException {
        // null rather than an EmulatorEndpoint: that argument is the whole difference between the
        // emulator path and the one a deployed job takes.
        return new DefaultSpannerDatabaseAccessFactory(
                        database, SpannerWriterOptions.defaults(), null)
                .create();
    }

    private static Refusal refusalFor(Mutation mutation) throws IOException {
        Refusal refusal = new Refusal();
        try (SpannerDatabaseAccess access = access(rejectionDatabase)) {
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
        }
        return refusal;
    }

    /**
     * What the service reported for each group of one request, keyed by the group's index.
     *
     * <p>A group the service said nothing about is <em>absent</em> rather than null, which is what
     * lets a caller tell "applied" apart from "never reported".
     */
    private static Map<Integer, String> outcomesOf(List<MutationGroup> groups) throws IOException {
        Map<Integer, String> outcomes = new LinkedHashMap<>();
        try (SpannerDatabaseAccess access = access(rejectionDatabase)) {
            access.batchWrite(
                    groups,
                    (groupIndex, status) -> {
                        StatusCode.Code code =
                                SpannerErrorClassifier.fromCanonicalCode(status.getCode());
                        outcomes.put(groupIndex, code == null ? "OK" : code.name());
                    });
        }
        return outcomes;
    }

    private static List<String> ids(String where) {
        List<String> ids = new ArrayList<>();
        query(rejectionDatabase, "SELECT id FROM orders " + where + " ORDER BY id")
                .forEach(row -> ids.add(row.getString(0)));
        return ids;
    }

    /**
     * Sends one batch-write request carrying roughly {@code targetBytes} of payload and reports
     * whether the service took it.
     *
     * <p>The payload is {@code STRING} rather than {@code BYTES} deliberately: Spanner carries a
     * {@code BYTES} value as a base64 string even over gRPC, so a byte payload inflates by four
     * thirds on the wire and the number this measures would stop being the number it names. An
     * ASCII string goes as itself. What the request costs beyond the payload — table and column
     * names, the group framing — is small change against megabytes, so "roughly" is the honest
     * word.
     */
    private static Outcome sendApproximately(int targetBytes) throws IOException {
        int rows = targetBytes / PAYLOAD_CHARS;
        List<MutationGroup> groups = new ArrayList<>(rows);
        // One payload string shared by every row, so the heap cost of a 110 MiB *request* is one
        // megabyte — the size is on the wire, not in memory.
        String payload = "x".repeat(PAYLOAD_CHARS);
        String probe = TestNames.runId();
        for (int i = 0; i < rows; i++) {
            groups.add(
                    MutationGroup.of(
                            Mutation.newInsertOrUpdateBuilder("blobs")
                                    .set("id")
                                    .to(probe + "-" + i)
                                    .set("payload")
                                    .to(payload)
                                    .build()));
        }
        // The batch knobs the writer would apply are not in the way here: this goes through the
        // database access directly, which sends exactly the groups it is given.
        try (SpannerDatabaseAccess access = access(blobsDatabase)) {
            List<StatusCode.Code> refusals = new ArrayList<>();
            access.batchWrite(
                    groups,
                    (groupIndex, status) -> {
                        StatusCode.Code code =
                                SpannerErrorClassifier.fromCanonicalCode(status.getCode());
                        if (code != null) {
                            refusals.add(code);
                        }
                    });
            return refusals.isEmpty()
                    ? Outcome.ok()
                    : Outcome.refused("per-group " + refusals.get(0));
        } catch (RuntimeException e) {
            return Outcome.refused(e.getMessage());
        }
    }

    /** Sends one request carrying {@code rawBytes} of {@code BYTES} payload, spread over rows. */
    private static Outcome sendRawBytes(int rawBytes) throws IOException {
        int rows = rawBytes / PAYLOAD_CHARS;
        List<MutationGroup> groups = new ArrayList<>(rows);
        // One array shared by every row, as with the string payload above.
        ByteArray payload = ByteArray.copyFrom(new byte[PAYLOAD_CHARS]);
        String probe = TestNames.runId();
        for (int i = 0; i < rows; i++) {
            groups.add(
                    MutationGroup.of(
                            Mutation.newInsertOrUpdateBuilder("blobs")
                                    .set("id")
                                    .to(probe + "-" + i)
                                    .set("raw")
                                    .to(payload)
                                    .build()));
        }
        try (SpannerDatabaseAccess access = access(blobsDatabase)) {
            access.batchWrite(groups, (groupIndex, status) -> {});
            return Outcome.ok();
        } catch (RuntimeException e) {
            return Outcome.refused(e.getMessage());
        }
    }

    /** The size gRPC says it received, out of "Received message larger than max (N vs. M)". */
    private static long reportedRequestSize(@Nullable String failure) {
        Matcher matcher =
                Pattern.compile("larger than max \\((\\d+) vs")
                        .matcher(failure == null ? "" : failure);
        assertThat(matcher.find())
                .as("the refusal should name the size it received: %s", failure)
                .isTrue();
        return Long.parseLong(matcher.group(1));
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

    /** Whether one sized request was taken, and what refused it when it was not. */
    private static final class Outcome {

        @Nullable private final String failure;
        private final boolean ran;

        private Outcome(@Nullable String failure, boolean ran) {
            this.failure = failure;
            this.ran = ran;
        }

        static Outcome ok() {
            return new Outcome(null, true);
        }

        static Outcome refused(@Nullable String failure) {
            // Never null when refused: the message is the only thing that tells a client-side
            // gRPC limit apart from the service's own, so a refusal with nothing to read is
            // recorded as such rather than as an accepted request.
            return new Outcome(failure == null ? "refused, no message" : failure, true);
        }

        static Outcome notRun() {
            return new Outcome(null, false);
        }

        boolean accepted() {
            return ran && failure == null;
        }

        @Override
        public String toString() {
            if (!ran) {
                return "not run";
            }
            return failure == null ? "accepted" : "refused: " + failure;
        }
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
