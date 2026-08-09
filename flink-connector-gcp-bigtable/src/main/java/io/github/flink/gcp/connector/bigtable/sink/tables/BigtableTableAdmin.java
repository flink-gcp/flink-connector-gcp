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

package io.github.flink.gcp.connector.bigtable.sink.tables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.GcRule;
import io.github.flink.gcp.connector.bigtable.sink.TableCreateOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Default {@link TableAdmin} backed by the Bigtable {@link BigtableTableAdminClient}.
 *
 * <p>Jobs whose destination table exists never construct a client (and never open its gRPC
 * channel). When auto-creation does trigger, the client is short-lived: opened for the one ensure
 * call and closed with it — together with its channel — so its resources are not held for the
 * writer's remaining lifetime for what is typically a one-shot event ({@link #close()} therefore
 * has nothing to release). With an emulator endpoint the short-lived clients connect to it over a
 * plaintext channel with no credentials.
 *
 * <p>Creation conflicts ({@code ALREADY_EXISTS}, the table or a family was created concurrently —
 * for example by a parallel subtask) are treated as success and resolved by re-reading: a lost
 * table-creation race falls through to the family reconciliation, and a lost family-addition race
 * re-reads and retries with the remaining absentees. The reconciliation reads the live families
 * first rather than blindly adding, because one {@code ModifyColumnFamilies} request is atomic — a
 * single already-existing family would fail the genuinely missing ones with it.
 *
 * <p>The reconciliation is self-bounding at the number of families the options declare, plus one: a
 * round that loses the race leaves strictly fewer of them missing. Spending that budget is a
 * contradiction rather than a slow ensure — a declared family disappearing between the read and the
 * modify, or a read not seeing what the modify reports — and it fails rather than looping on,
 * because a loop with no end stops the task thread and surfaces, if at all, as checkpoints that
 * stop completing and a task that will not cancel, never as the reconciliation that caused it,
 * whereas the failure becomes an {@link IOException} the writer's recovery schedule already spends
 * an attempt on.
 *
 * <p>The client retries neither {@code CreateTable} nor {@code ModifyColumnFamilies} (their
 * retryable-code sets are empty), so a transiently failed ensure fails this call; the writer's
 * recovery budget is what bounds re-provocation, not this class.
 */
@Internal
public class BigtableTableAdmin implements TableAdmin {

    private static final Logger LOG = LoggerFactory.getLogger(BigtableTableAdmin.class);

    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /** Creates an admin using application-default credentials. */
    public BigtableTableAdmin() {
        this(null);
    }

    /**
     * Creates the admin.
     *
     * @param emulatorEndpoint the emulator endpoint (plaintext, no credentials), or {@code null}
     *     for production Bigtable with application-default credentials
     */
    public BigtableTableAdmin(@Nullable EmulatorEndpoint emulatorEndpoint) {
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public EnsureResult ensureTable(TableDestination destination, TableCreateOptions options)
            throws IOException {
        try (BigtableTableAdminClient client = newClient(destination)) {
            return ensureWith(
                    destination,
                    options,
                    client::createTable,
                    tableId -> familyIdsOf(client, tableId),
                    client::modifyFamilies);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to create Bigtable table "
                            + destination
                            + " or its missing column families.",
                    e);
        }
    }

    /**
     * Ensures the table through the three admin operations, taken as functional values rather than
     * as the client that performs them.
     *
     * <p><b>Because that is the only seam a test can drive</b> (#414), the same shape and the same
     * reason as {@code BigtableBatcherAdapter}'s (ADR-0047). {@link BigtableTableAdminClient} is
     * final, this repository uses no mocking framework, and the client here is built inside {@link
     * #ensureTable} and closed with it, so nothing can hand in a scripted one. Nor can the emulator
     * substitute: what needs driving is a <em>concurrent</em> family addition landing between one
     * call's read and its modify, and nothing short of interposing on the RPC stream can time it.
     * Measured on the pre-seam code (2026-08-09): a mutant that returned instead of looping, and
     * one that looped without re-reading, each survived this module's whole non-gated suite — every
     * test that reaches this class; the gated real-GCP auto-creation cases are single-threaded and
     * would not produce the race either.
     *
     * <p>The read yields the live family ids rather than the client's {@code Table}: that type has
     * no public constructor, so a test would have to mint one through its {@code @InternalApi}
     * {@code fromProto}. What that leaves outside the seam is one projection, which {@code
     * BigtableTableAdminEmulatorITCase} pins in both directions — its no-op case fails if the read
     * reports too few families, its amend case if it reports too many. That ITCase is also why the
     * method references binding this call to a real client are covered, unlike the untested wiring
     * #321 found: it drives {@link #ensureTable} itself down the creation, the addition and the
     * no-op path.
     */
    @VisibleForTesting
    static EnsureResult ensureWith(
            TableDestination destination,
            TableCreateOptions options,
            Consumer<CreateTableRequest> createTable,
            Function<String, Set<String>> readFamilyIds,
            Consumer<ModifyColumnFamiliesRequest> modifyFamilies) {
        try {
            createTable.accept(toCreateTableRequest(destination, options));
            LOG.info(
                    "Created Bigtable table {} with column families {}",
                    destination,
                    options.getColumnFamilies().keySet());
            return EnsureResult.created();
        } catch (AlreadyExistsException e) {
            LOG.info("Bigtable table {} already exists, not creating it", destination);
        }
        return addMissingFamilies(destination, options, readFamilyIds, modifyFamilies);
    }

    /**
     * Adds the declared families an existing table lacks, in one atomic request per round. A round
     * that loses the race to a parallel subtask ({@code ALREADY_EXISTS}: the atomic request added
     * nothing) re-reads and retries with what is still missing.
     *
     * <p>The round budget is the declared family count plus one, which is the exact bound rather
     * than a chosen cap: a losing round means at least one family this call found missing is now
     * present, so the missing set shrinks strictly and is a subset of the declared families. At
     * most that many rounds can lose, and one more either adds the remainder or finds nothing to
     * add. Spending it is therefore not a slow ensure but a contradiction — see the tripwire's own
     * message — and it fails rather than spinning, because a loop with no end holds the task thread
     * and would be reported only as checkpoints that stop completing, while the failure is a retry
     * the writer's recovery schedule already knows how to spend.
     */
    private static EnsureResult addMissingFamilies(
            TableDestination destination,
            TableCreateOptions options,
            Function<String, Set<String>> readFamilyIds,
            Consumer<ModifyColumnFamiliesRequest> modifyFamilies) {
        int rounds = options.getColumnFamilies().size() + 1;
        // Carried out of the loop for the tripwire's message: which families were still absent is
        // the one thing an operator meeting it can act on, and the last round is the only round
        // that knows.
        Set<String> stillMissing = options.getColumnFamilies().keySet();
        for (int budget = rounds; budget > 0; budget--) {
            Set<String> existing = readFamilyIds.apply(destination.getTable());
            Map<String, GcRule> missing = new LinkedHashMap<>(options.getColumnFamilies());
            missing.keySet().removeAll(existing);
            if (missing.isEmpty()) {
                return EnsureResult.familiesAdded(0);
            }
            stillMissing = missing.keySet();
            try {
                modifyFamilies.accept(toModifyColumnFamiliesRequest(destination, missing));
                LOG.info(
                        "Added column families {} to Bigtable table {}",
                        missing.keySet(),
                        destination);
                return EnsureResult.familiesAdded(missing.size());
            } catch (AlreadyExistsException e) {
                LOG.info(
                        "A column family of Bigtable table {} was added concurrently; re-reading"
                                + " and retrying the remainder",
                        destination);
            }
        }
        throw new IllegalStateException(
                "The column families "
                        + stillMissing
                        + " declared for Bigtable table "
                        + destination
                        + " were still missing after "
                        + rounds
                        + " reconciliation rounds, each of which was told the families it was"
                        + " adding already exist. A round only repeats when a family it read as"
                        + " missing was added concurrently, so a declared family is being deleted"
                        + " between the read and the modify, or the read is not seeing what the"
                        + " modify reports.");
    }

    /** Reads the ids of the column families the given table currently has. */
    private static Set<String> familyIdsOf(BigtableTableAdminClient client, String tableId) {
        return client.getTable(tableId).getColumnFamilies().stream()
                .map(ColumnFamily::getId)
                .collect(Collectors.toSet());
    }

    /** Translates the create options into the table-creation request. */
    @VisibleForTesting
    static CreateTableRequest toCreateTableRequest(
            TableDestination destination, TableCreateOptions options) {
        CreateTableRequest request = CreateTableRequest.of(destination.getTable());
        options.getColumnFamilies()
                .forEach(
                        (name, rule) -> {
                            if (rule == null) {
                                request.addFamily(name);
                            } else {
                                request.addFamily(name, toGcRule(rule));
                            }
                        });
        return request;
    }

    /** Translates the given families into one atomic family-addition request. */
    @VisibleForTesting
    static ModifyColumnFamiliesRequest toModifyColumnFamiliesRequest(
            TableDestination destination, Map<String, GcRule> families) {
        ModifyColumnFamiliesRequest request =
                ModifyColumnFamiliesRequest.of(destination.getTable());
        families.forEach(
                (name, rule) -> {
                    if (rule == null) {
                        request.addFamily(name);
                    } else {
                        request.addFamily(name, toGcRule(rule));
                    }
                });
        return request;
    }

    /**
     * Translates the sink's serializable rule into the client's model. The age conversion goes
     * seconds-and-nanos to seconds-and-nanos, so no magnitude a {@link Duration} can hold overflows
     * it.
     */
    @VisibleForTesting
    static GCRules.GCRule toGcRule(GcRule rule) {
        switch (rule.getKind()) {
            case MAX_VERSIONS:
                return GCRules.GCRULES.maxVersions(rule.getMaxVersions());
            case MAX_AGE:
                Duration maxAge = rule.getMaxAge();
                return GCRules.GCRULES.maxAge(
                        org.threeten.bp.Duration.ofSeconds(maxAge.getSeconds(), maxAge.getNano()));
            case UNION:
                GCRules.UnionRule union = GCRules.GCRULES.union();
                for (GcRule nested : rule.getRules()) {
                    union = union.rule(toGcRule(nested));
                }
                return union;
            case INTERSECTION:
                GCRules.IntersectionRule intersection = GCRules.GCRULES.intersection();
                for (GcRule nested : rule.getRules()) {
                    intersection = intersection.rule(toGcRule(nested));
                }
                return intersection;
        }
        throw new IllegalStateException("Unknown GcRule kind: " + rule.getKind());
    }

    @Override
    public void close() {
        // Clients are short-lived within ensureTable; there is nothing to release here.
    }

    private BigtableTableAdminClient newClient(TableDestination destination) throws IOException {
        try {
            BigtableTableAdminSettings.Builder settings =
                    emulatorEndpoint == null
                            ? BigtableTableAdminSettings.newBuilder()
                            : BigtableTableAdminSettings.newBuilderForEmulator(
                                    emulatorEndpoint.getHost(), emulatorEndpoint.getPort());
            return BigtableTableAdminClient.create(
                    settings.setProjectId(destination.getProject())
                            .setInstanceId(destination.getInstance())
                            .build());
        } catch (IOException | RuntimeException e) {
            throw new IOException("Failed to create the Bigtable admin client", e);
        }
    }
}
