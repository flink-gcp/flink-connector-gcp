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

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;

/**
 * The deterministic identity under which a query job may be reused across a re-plan.
 *
 * <p><b>The key is the Flink job name, not the Flink job id</b>, because the name is the one
 * identifier the user explicitly controls: {@code StreamExecutionEnvironment#execute(String)} sets
 * it, and "rename the job to force a fresh result" is a contract a user can act on. The JobID
 * cannot carry that contract — when it changes depends on how the job is deployed (random per
 * submission in a session cluster, but derived from the HA cluster id in a high-availability
 * application deployment, where it is the <em>same</em> across redeploys), and a user cannot see
 * which of the two they have.
 *
 * <p><b>The digest is over everything the runner reads</b> — the original job name and every {@link
 * QuerySpec} field, plus the window itself — so two sources whose ids collide are submitting the
 * same query to the same place with the same setting (up to the sixteen-hex digest, the load
 * runner's precedent), and sharing the job is correct. That is what makes the job name safe as a
 * key even at Flink's default name: two unrelated pipelines that never set one still differ in
 * their queries.
 *
 * <p><b>The window rides in the id as a bucket</b> ({@code now / window}), so an id from an older
 * window is never <em>submitted</em> again — which is what keeps BigQuery's six-month retention of
 * job ids from ever answering {@code ALREADY_EXISTS} for a job too old to attach to. Attaching is
 * governed separately: a job found under the previous bucket's id is reused only if {@link
 * #isWithinWindow} says its creation time is inside the window, so a failover that straddles a
 * bucket rollover still deduplicates instead of paying for the query again.
 */
@Internal
public final class QueryJobIdentity {

    /**
     * The prefix every query-source job id carries, deterministic or random — one spelling, so the
     * two id families cannot drift apart in the console.
     */
    static final String PREFIX = "flink_bigquery_source_";

    /**
     * How much of the sanitised job name the id keeps.
     *
     * <p>Readability only: the digest is what keeps distinct names distinct, so truncation cannot
     * collide two ids. Sixty-four characters keeps the whole id — which is also the result table's
     * name — comfortably inside BigQuery's limits with the digest and bucket appended.
     */
    private static final int NAME_LIMIT = 64;

    private final String currentJobId;
    private final String previousJobId;
    private final long windowMillis;

    private QueryJobIdentity(String currentJobId, String previousJobId, long windowMillis) {
        this.currentJobId = currentJobId;
        this.previousJobId = previousJobId;
        this.windowMillis = windowMillis;
    }

    /**
     * Builds the identity, or answers {@code null} where the job name is not available.
     *
     * <p>{@code null} is the caller's signal to fall back to a random id — today's behaviour, which
     * is correct and merely forgoes the reuse — rather than fail a job over a metric variable the
     * runtime did not fill in.
     *
     * @param jobName the Flink job name, or {@code null} where the enumerator could not read one
     * @param spec the query the identity is for
     * @param window how long attempts reuse each other's query job
     * @param nowMillis the current time, in epoch milliseconds
     * @return the identity, or {@code null} without a job name
     */
    @Nullable
    public static QueryJobIdentity of(
            @Nullable String jobName, QuerySpec spec, Duration window, long nowMillis) {
        Preconditions.checkNotNull(spec, "spec must not be null");
        Preconditions.checkNotNull(window, "window must not be null");
        if (jobName == null || jobName.isEmpty()) {
            return null;
        }
        long windowMillis = window.toMillis();
        Preconditions.checkArgument(windowMillis > 0, "window must be positive");
        long bucket = nowMillis / windowMillis;
        String base = PREFIX + sanitize(jobName) + "_" + digest(jobName, spec, windowMillis) + "_";
        return new QueryJobIdentity(base + bucket, base + (bucket - 1), windowMillis);
    }

    /** Returns the id a job is submitted under, and looked up under first. */
    public String getCurrentJobId() {
        return currentJobId;
    }

    /**
     * Returns the previous window's id, which is only ever <em>looked up</em>, never submitted —
     * submitting it is what would meet BigQuery's six-month id retention.
     */
    public String getPreviousJobId() {
        return previousJobId;
    }

    /**
     * Returns whether a job created then is still inside the reuse window now.
     *
     * <p>A creation time in the future — clock skew between two JobManagers — counts as inside:
     * skew is bounded by far less than any usable window, and treating it as outside would run the
     * query again over a clock disagreement.
     *
     * @param creationTimeMillis the job's creation time, in epoch milliseconds
     * @param nowMillis the current time, in epoch milliseconds
     * @return whether the job may be reused
     */
    public boolean isWithinWindow(long creationTimeMillis, long nowMillis) {
        return nowMillis - creationTimeMillis <= windowMillis;
    }

    /**
     * Replaces what a BigQuery job id cannot carry, keeping the name readable in the console.
     *
     * <p>The sanitised form is lossy on purpose — {@code my job} and {@code my-job} both become
     * {@code my_job} — because distinctness is the digest's to keep, over the <em>original</em>
     * name. The id doubles as the result table's name, which is why the alphabet is letters, digits
     * and underscores rather than the job id's slightly wider one.
     */
    private static String sanitize(String jobName) {
        String sanitized = jobName.replaceAll("[^A-Za-z0-9_]", "_");
        return sanitized.length() <= NAME_LIMIT ? sanitized : sanitized.substring(0, NAME_LIMIT);
    }

    /**
     * Digests everything the runner reads, so equal ids mean equal jobs.
     *
     * <p>Each field is length-prefixed before the concatenation, so no two distinct inputs
     * concatenate alike whatever characters a query carries, and a {@code null} is encoded
     * distinctly from every string including the empty one. Sixteen hex characters follows {@code
     * LoadJobOrchestrator}'s choice for its deterministic load-job ids.
     */
    private static String digest(String jobName, QuerySpec spec, long windowMillis) {
        String material =
                encode(jobName)
                        + encode(spec.getSql())
                        + encode(spec.getProject())
                        + encode(spec.getLocation())
                        + encode(spec.getResultDataset())
                        + encode(Long.toString(windowMillis));
        return sha256Hex(material).substring(0, 16);
    }

    /** Length-prefixes a field, encoding {@code null} distinctly from every string. */
    private static String encode(@Nullable String value) {
        return value == null ? "-:" : value.length() + ":" + value;
    }

    // Mirrors LoadJobOrchestrator's private copy; the two live on opposite sides of the
    // sink/source split, and a shared home for ten lines would be a package neither owns.
    private static String sha256Hex(String value) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // Every JVM is required to ship SHA-256.
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
        StringBuilder hex = new StringBuilder();
        for (byte b : digest.digest(value.getBytes(StandardCharsets.UTF_8))) {
            hex.append(String.format("%02x", b));
        }
        return hex.toString();
    }
}
