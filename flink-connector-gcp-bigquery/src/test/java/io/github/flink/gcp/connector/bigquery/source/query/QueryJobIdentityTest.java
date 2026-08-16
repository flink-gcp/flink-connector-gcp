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

package io.github.flink.gcp.connector.bigquery.source.query;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the reuse id is derived from, and what it must never collapse.
 *
 * <p>The properties here are the whole safety argument of keying on the job name: an id built twice
 * from the same inputs is the same (or a failover would find nothing to reuse), and an id built
 * from inputs differing in <em>any</em> field the runner reads is different (or two jobs would
 * share a result one of them did not ask for).
 */
class QueryJobIdentityTest {

    private static final Duration DAY = Duration.ofHours(24);
    private static final QuerySpec SPEC = new QuerySpec("SELECT 1", "p", null, null);

    /** Some instant well inside a bucket, so a small time step stays in the same bucket. */
    private static final long NOW = DAY.toMillis() * 20_000 + DAY.toMillis() / 2;

    @Test
    void theSameInputsAlwaysDeriveTheSameId() {
        QueryJobIdentity first = QueryJobIdentity.of("pipeline", SPEC, DAY, NOW);
        // A failover re-derives minutes later, not at the same millisecond.
        QueryJobIdentity second =
                QueryJobIdentity.of("pipeline", SPEC, DAY, NOW + Duration.ofMinutes(5).toMillis());

        assertThat(first.getCurrentJobId()).isEqualTo(second.getCurrentJobId());
    }

    @Test
    void everyFieldTheRunnerReadsChangesTheId() {
        String base = QueryJobIdentity.of("pipeline", SPEC, DAY, NOW).getCurrentJobId();

        assertThat(QueryJobIdentity.of("renamed", SPEC, DAY, NOW).getCurrentJobId())
                .as("the job name")
                .isNotEqualTo(base);
        assertThat(
                        QueryJobIdentity.of(
                                        "pipeline",
                                        new QuerySpec("SELECT 2", "p", null, null),
                                        DAY,
                                        NOW)
                                .getCurrentJobId())
                .as("the query")
                .isNotEqualTo(base);
        assertThat(
                        QueryJobIdentity.of(
                                        "pipeline",
                                        new QuerySpec("SELECT 1", "p2", null, null),
                                        DAY,
                                        NOW)
                                .getCurrentJobId())
                .as("the project")
                .isNotEqualTo(base);
        assertThat(
                        QueryJobIdentity.of(
                                        "pipeline",
                                        new QuerySpec("SELECT 1", "p", "EU", null),
                                        DAY,
                                        NOW)
                                .getCurrentJobId())
                .as("the location")
                .isNotEqualTo(base);
        assertThat(
                        QueryJobIdentity.of(
                                        "pipeline",
                                        new QuerySpec("SELECT 1", "p", null, "scratch"),
                                        DAY,
                                        NOW)
                                .getCurrentJobId())
                .as("the result dataset")
                .isNotEqualTo(base);
        // Two windows whose buckets coincide at this instant (12h / 10h = 12h / 11h = bucket 1),
        // so the digest is the only thing keeping the two configurations apart — a bucket-only
        // separation would pass with the window dropped from the digest.
        long now = Duration.ofHours(12).toMillis();
        assertThat(
                        QueryJobIdentity.of("pipeline", SPEC, Duration.ofHours(10), now)
                                .getCurrentJobId())
                .as("the window setting — two differently-configured jobs must not share an id")
                .isNotEqualTo(
                        QueryJobIdentity.of("pipeline", SPEC, Duration.ofHours(11), now)
                                .getCurrentJobId());
    }

    @Test
    void sanitizingTheNameDoesNotCollapseDistinctNames() {
        // Both sanitize to my_job; the digest over the original names is what keeps them apart.
        QueryJobIdentity spaced = QueryJobIdentity.of("my job", SPEC, DAY, NOW);
        QueryJobIdentity dashed = QueryJobIdentity.of("my-job", SPEC, DAY, NOW);

        assertThat(spaced.getCurrentJobId()).isNotEqualTo(dashed.getCurrentJobId());
        // The readable part is still the sanitised name.
        assertThat(spaced.getCurrentJobId()).contains("_my_job_");
    }

    @Test
    void theIdIsALegalBigQueryJobIdAndTableName() {
        // The suffix doubles as the result table's name, whose alphabet is the narrower of the
        // two — letters, digits and underscores. A long unicode name must still fit.
        QueryJobIdentity identity =
                QueryJobIdentity.of("ジョブ: really → long ".repeat(20), SPEC, DAY, NOW);

        assertThat(identity.getCurrentJobId()).matches("[A-Za-z0-9_]+");
        assertThat(identity.getCurrentJobId().length()).isLessThan(200);
    }

    @Test
    void theBucketRidesInTheIdAndRollsWithTheWindow() {
        QueryJobIdentity before = QueryJobIdentity.of("pipeline", SPEC, DAY, NOW);
        QueryJobIdentity after = QueryJobIdentity.of("pipeline", SPEC, DAY, NOW + DAY.toMillis());

        assertThat(before.getCurrentJobId()).isNotEqualTo(after.getCurrentJobId());
        // The rolled-over window's previous id is exactly the older window's current id, which is
        // what lets a failover straddling the rollover find the job it is deduplicating against.
        assertThat(after.getPreviousJobId()).isEqualTo(before.getCurrentJobId());
    }

    @Test
    void theWindowCheckIsExactAndToleratesClockSkew() {
        QueryJobIdentity identity = QueryJobIdentity.of("pipeline", SPEC, DAY, NOW);

        assertThat(identity.isWithinWindow(NOW - DAY.toMillis(), NOW))
                .as("a job exactly a window old is still inside")
                .isTrue();
        assertThat(identity.isWithinWindow(NOW - DAY.toMillis() - 1, NOW))
                .as("a millisecond past the window is outside")
                .isFalse();
        assertThat(identity.isWithinWindow(NOW + 1_000, NOW))
                .as("a creation time in the future is clock skew, not staleness")
                .isTrue();
    }

    @Test
    void withoutAJobNameThereIsNoIdentity() {
        assertThat(QueryJobIdentity.of(null, SPEC, DAY, NOW)).isNull();
        assertThat(QueryJobIdentity.of("", SPEC, DAY, NOW)).isNull();
    }
}
