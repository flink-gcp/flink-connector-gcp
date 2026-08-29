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

package io.github.flink.gcp.connector.spanner;

import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.spanner.admin.instance.v1.Instance.Edition;
import io.github.flink.gcp.connector.testutils.TestNames;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Shared harness for the gated integration tests that run against real Cloud Spanner — what the
 * emulator cannot show: which status the service answers each bad mutation with, which query shapes
 * it will plan a partitioned read for, how many partitions it plans, and whether Data Boost serves
 * a read at all. Every emulator test goes through {@code emulatorEndpoint(...)}, so this is also
 * the only place the production client-construction path over application-default credentials runs.
 *
 * <p>Clients authenticate with application-default credentials; the project comes from {@code
 * SPANNER_IT_PROJECT}. There is no companion variable, because nothing persistent is provisioned:
 * an instance bills for as long as it exists, so each class creates one of its own and deletes it
 * in {@link AfterAll} rather than running inside a standing one.
 *
 * <p>The instance is the smallest and cheapest thing that can serve this suite: {@link
 * #PROCESSING_UNITS} processing units, which is the floor for a regional configuration, in the
 * {@code STANDARD} edition. The edition is set rather than defaulted because it is the cheapest one
 * that carries Data Boost — {@code SpannerSourceRealGcpITCase} reads through Data Boost on this
 * very instance, so that is measured here rather than taken from the editions page. A free-trial
 * instance, which would be cheaper still, is not an option: Data Boost is one of the few features
 * its documentation lists as unsupported.
 *
 * <p><b>Per class, not per run</b>, following {@code AbstractBigtableRealGcpITCase} and {@code
 * docs/adr/0044}: the {@code integration-tests} surefire execution that {@code just e2e} invokes
 * runs with {@code forkCount=2} and {@code reuseForks=true}, so a shared holder would be raced by
 * the two forks, a single class must stay runnable by hand, and the best-effort deletion below
 * tracks per class. A class therefore creates its databases once, in its own {@link BeforeAll}, and
 * shares them across its tests — on the service a database takes seconds to create, where the
 * emulator tests can afford one per test method.
 *
 * <p>Deletion is best-effort, so instance names carry their creation time and {@link
 * #sweepStaleInstances} deletes anything older than {@link #STALE_AFTER} before creating this
 * class's own. That threshold is far above the E2E workflow's timeout, so the sweep cannot reach a
 * live run's instance. {@code scripts/sweep-e2e.sh} reads the prefix and the threshold out of this
 * file rather than repeating them, so the two sweeps cannot drift apart.
 *
 * <p>The {@code @EnabledIfEnvironmentVariable} gate lives on every concrete class, never here:
 * {@code scripts/e2e-gated-its.sh} discovers the suite by parsing the annotation on each file and
 * then expects a surefire report per matching file, which an abstract class never produces. The
 * {@code gated} tag beside it (issue #245) has to stay on the concrete classes for the same reason,
 * even though JUnit would inherit it from here: {@code --check-tags} checks both annotations per
 * file, so hoisting one leaves the other unpaired.
 *
 * <p>The timeout runs in a separate thread because the default mode cannot end a wait that ignores
 * interruption, which is how #951 outlived its own deadline. ADR-0119 records the measurement and
 * the fork-level ceiling that covers what abandoning a thread cannot.
 */
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
public abstract class AbstractSpannerRealGcpITCase {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSpannerRealGcpITCase.class);

    /** The project the suite runs against; null when the gate is off (the tests then skip). */
    protected static final String PROJECT = System.getenv("SPANNER_IT_PROJECT");

    /**
     * Identifies an instance as this suite's, for the sweep. Instance ids are 2–64 characters of
     * lowercase letters, digits and hyphens starting with a letter, which {@code flink-it-} plus
     * ten digits of epoch seconds plus an eight-character run id fits with room to spare.
     */
    private static final String INSTANCE_PREFIX = "flink-it-";

    /** Where the ephemeral instance goes — in the region the other IT resources already use. */
    private static final String INSTANCE_CONFIG = "regional-us-central1";

    /** The floor for a regional configuration, and far more than this suite's few thousand rows. */
    private static final int PROCESSING_UNITS = 100;

    /** An instance older than this belongs to a run that crashed; see the class javadoc. */
    private static final Duration STALE_AFTER = Duration.ofHours(2);

    private static String instanceId;
    private static Spanner spanner;

    @BeforeAll
    protected static void createInstanceAndClient() throws Exception {
        // No emulator host, which is what makes this the application-default-credentials path —
        // see SpannerClients, where that one branch is the whole difference.
        spanner = SpannerOptions.newBuilder().setProjectId(PROJECT).build().getService();
        sweepStaleInstances();

        instanceId = INSTANCE_PREFIX + Instant.now().getEpochSecond() + "-" + TestNames.runId();
        LOG.info("Creating ephemeral Spanner instance {} in {}", instanceId, INSTANCE_CONFIG);
        spanner.getInstanceAdminClient()
                .createInstance(
                        InstanceInfo.newBuilder(InstanceId.of(PROJECT, instanceId))
                                .setInstanceConfigId(InstanceConfigId.of(PROJECT, INSTANCE_CONFIG))
                                .setProcessingUnits(PROCESSING_UNITS)
                                .setEdition(Edition.STANDARD)
                                .setDisplayName("flink-connector-gcp E2E")
                                .build())
                .get();
    }

    @AfterAll
    protected static void deleteInstanceAndCloseClient() {
        try {
            // The instance goes first, before the client is closed: it is the only part of this
            // fixture that costs money, and a client throwing on close must not be able to skip
            // its deletion. Deleting it takes its databases with it, so there is nothing else to
            // clean up.
            if (spanner != null && instanceId != null) {
                spanner.getInstanceAdminClient().deleteInstance(instanceId);
            }
        } catch (RuntimeException e) {
            LOG.warn(
                    "Failed to delete instance {}; a later run's sweep reclaims it", instanceId, e);
        } finally {
            if (spanner != null) {
                spanner.close();
            }
        }
    }

    /**
     * Deletes instances this suite created that are older than {@link #STALE_AFTER}, so a run
     * killed before its {@link AfterAll} leaves a cost that stops at the next run rather than
     * standing indefinitely.
     *
     * <p>Two forks sweeping at once can both pick the same instance; the loser sees the delete fail
     * and logs it. An id that carries no parsable timestamp is left alone: this deletes instances,
     * and a name it cannot date is a name it does not understand.
     */
    private static void sweepStaleInstances() {
        Instant cutoff = Instant.now().minus(STALE_AFTER);
        for (Instance instance : spanner.getInstanceAdminClient().listInstances().iterateAll()) {
            String id = instance.getId().getInstance();
            Instant created = createdAt(id);
            if (created == null || !created.isBefore(cutoff)) {
                continue;
            }
            LOG.warn("Sweeping stale instance {}, created {}", id, created);
            try {
                spanner.getInstanceAdminClient().deleteInstance(id);
            } catch (RuntimeException e) {
                LOG.warn("Failed to sweep {}", id, e);
            }
        }
    }

    /** The creation time encoded in an instance id, or null if this suite did not create it. */
    @Nullable
    private static Instant createdAt(String id) {
        if (!id.startsWith(INSTANCE_PREFIX)) {
            return null;
        }
        String remainder = id.substring(INSTANCE_PREFIX.length());
        int end = remainder.indexOf('-');
        try {
            return Instant.ofEpochSecond(
                    Long.parseLong(end < 0 ? remainder : remainder.substring(0, end)));
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Creates a database of the given dialect with the given DDL in the ephemeral instance. */
    protected static DatabaseDestination createDatabase(Dialect dialect, String... ddl)
            throws Exception {
        // Lower case, and underscores rather than the hyphens TestNames.unique would give: a
        // database id must match [a-z][a-z0-9_-]{1,29}.
        String databaseId =
                "db_"
                        + TestNames.unique("")
                                .replace("-", "")
                                .toLowerCase(Locale.ROOT)
                                .substring(0, 20);
        DatabaseAdminClient admin = spanner.getDatabaseAdminClient();
        // The second argument is the CREATE DATABASE *statement*, not the id — and the two
        // dialects spell and quote it differently, which the client library's own Dialect knows.
        //
        // The DDL goes in a second call for PostgreSQL, and that is a measured emulator
        // deviation rather than caution (2026-08-10): the service answers a PostgreSQL
        // CreateDatabase carrying extra statements with "DDL statements other than <CREATE
        // DATABASE> are not allowed in database creation request for PostgreSQL-enabled
        // databases", where the emulator applies them. The one-call form is kept for GoogleSQL
        // because that is what the emulator tests exercise, so the two harnesses stay comparable
        // on the dialect they share.
        boolean ddlInCreate = dialect != Dialect.POSTGRESQL;
        admin.createDatabase(
                        instanceId,
                        dialect.createDatabaseStatementFor(databaseId),
                        dialect,
                        ddlInCreate ? Arrays.asList(ddl) : Collections.emptyList())
                .get();
        if (!ddlInCreate && ddl.length > 0) {
            admin.updateDatabaseDdl(instanceId, databaseId, Arrays.asList(ddl), null).get();
        }
        return DatabaseDestination.of(PROJECT, instanceId, databaseId);
    }

    /** Runs a query against the database and materializes its rows. */
    protected static List<Struct> query(DatabaseDestination database, String sql) {
        List<Struct> rows = new ArrayList<>();
        try (ResultSet resultSet = client(database).singleUse().executeQuery(Statement.of(sql))) {
            while (resultSet.next()) {
                rows.add(resultSet.getCurrentRowAsStruct());
            }
        }
        return rows;
    }

    /**
     * Applies mutations through the harness's own client, for arranging a test's starting state.
     */
    protected static DatabaseClient client(DatabaseDestination database) {
        return spanner.getDatabaseClient(
                DatabaseId.of(
                        database.getProject(), database.getInstance(), database.getDatabase()));
    }

    /** The harness's client handle, for a test that needs a batch transaction of its own. */
    protected static Spanner spanner() {
        return spanner;
    }
}
