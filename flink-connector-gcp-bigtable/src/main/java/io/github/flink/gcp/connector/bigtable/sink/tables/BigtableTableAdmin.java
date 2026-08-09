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
 * re-reads and retries with the remaining absentees. Each extra round requires a fresh concurrent
 * addition of a still-missing family, so the loop terminates unless a third party keeps adding and
 * deleting the very families this sink declares — perpetual external churn, accepted unbounded
 * rather than capped. The reconciliation reads the live families first rather than blindly adding,
 * because one {@code ModifyColumnFamilies} request is atomic — a single already-existing family
 * would fail the genuinely missing ones with it.
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
            try {
                client.createTable(toCreateTableRequest(destination, options));
                LOG.info(
                        "Created Bigtable table {} with column families {}",
                        destination,
                        options.getColumnFamilies().keySet());
                return EnsureResult.created();
            } catch (AlreadyExistsException e) {
                LOG.info("Bigtable table {} already exists, not creating it", destination);
            }
            return addMissingFamilies(client, destination, options);
        } catch (RuntimeException e) {
            throw new IOException(
                    "Failed to create Bigtable table "
                            + destination
                            + " or its missing column families.",
                    e);
        }
    }

    /**
     * Adds the declared families an existing table lacks, in one atomic request per round. A round
     * that loses the race to a parallel subtask ({@code ALREADY_EXISTS}: the atomic request added
     * nothing) re-reads and retries with what is still missing.
     */
    private static EnsureResult addMissingFamilies(
            BigtableTableAdminClient client,
            TableDestination destination,
            TableCreateOptions options) {
        while (true) {
            Set<String> existing =
                    client.getTable(destination.getTable()).getColumnFamilies().stream()
                            .map(ColumnFamily::getId)
                            .collect(Collectors.toSet());
            Map<String, GcRule> missing = new LinkedHashMap<>(options.getColumnFamilies());
            missing.keySet().removeAll(existing);
            if (missing.isEmpty()) {
                return EnsureResult.familiesAdded(0);
            }
            try {
                client.modifyFamilies(toModifyColumnFamiliesRequest(destination, missing));
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
