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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.options.OptionChecks;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A garbage-collection rule for a column family the sink creates, mirroring the four rule shapes
 * Bigtable's admin API takes: a version cap, an age cap, and their union and intersection.
 *
 * <p>This is the sink's own value type rather than the client's {@code GCRules.GCRule} because it
 * travels in the job graph: {@link TableCreateOptions} is serialized into the sink's configuration,
 * and the client's rule models are not {@link Serializable}. The translation happens where the
 * table is created.
 *
 * <p>A family declared without a rule keeps Bigtable's default of collecting nothing, which for
 * this sink is a decision, not an omission: the sink is at-least-once, a replay writes the same
 * cells again, and the garbage-collection policy is what decides whether those duplicate versions
 * accumulate forever. {@code union(maxVersions(1), maxAge(...))} is the usual shape for keeping
 * only the latest cell.
 *
 * <p>Validation is shape-only (positivity, arity); nesting depth and any service-side limits are
 * left to Bigtable, whose rejection names what it refused. Instances are immutable.
 */
@PublicEvolving
public final class GcRule implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The rule shapes Bigtable's admin API takes. */
    @PublicEvolving
    public enum Kind {
        MAX_VERSIONS,
        MAX_AGE,
        UNION,
        INTERSECTION
    }

    private final Kind kind;
    @Nullable private final Integer maxVersions;
    @Nullable private final Duration maxAge;
    @Nullable private final List<GcRule> rules;

    private GcRule(
            Kind kind,
            @Nullable Integer maxVersions,
            @Nullable Duration maxAge,
            @Nullable List<GcRule> rules) {
        this.kind = kind;
        this.maxVersions = maxVersions;
        this.maxAge = maxAge;
        this.rules = rules;
    }

    /**
     * A rule collecting all but the newest {@code maxVersions} versions of a cell.
     *
     * @param maxVersions the number of versions to keep, positive
     * @return the rule
     */
    public static GcRule maxVersions(int maxVersions) {
        Preconditions.checkArgument(maxVersions > 0, "maxVersions must be positive");
        return new GcRule(Kind.MAX_VERSIONS, maxVersions, null, null);
    }

    private static GcRule composite(Kind kind, GcRule... rules) {
        Preconditions.checkNotNull(rules, "rules must not be null");
        // Two is the floor because a composite of one is that rule, and of zero is meaningless —
        // requiring the caller to say what they meant beats silently normalizing either.
        Preconditions.checkArgument(
                rules.length >= 2,
                "%s takes at least two rules",
                kind.name().toLowerCase(Locale.ROOT));
        for (GcRule rule : rules) {
            Preconditions.checkNotNull(rule, "rules must not contain null");
        }
        return new GcRule(
                kind, null, null, Collections.unmodifiableList(Arrays.asList(rules.clone())));
    }

    /**
     * A rule collecting cells older than {@code maxAge}.
     *
     * @param maxAge the age past which cells are collected, positive
     * @return the rule
     */
    public static GcRule maxAge(Duration maxAge) {
        return new GcRule(Kind.MAX_AGE, null, OptionChecks.checkPositive(maxAge, "maxAge"), null);
    }

    /**
     * A rule collecting cells any of the given rules would collect.
     *
     * @param rules the rules to union, at least two
     * @return the rule
     */
    public static GcRule union(GcRule... rules) {
        return composite(Kind.UNION, rules);
    }

    /**
     * A rule collecting only cells every one of the given rules would collect.
     *
     * @param rules the rules to intersect, at least two
     * @return the rule
     */
    public static GcRule intersection(GcRule... rules) {
        return composite(Kind.INTERSECTION, rules);
    }

    /** Returns the rule's shape. */
    public Kind getKind() {
        return kind;
    }

    /**
     * Returns the version cap of a {@link Kind#MAX_VERSIONS} rule, or {@code null} for other kinds.
     */
    @Nullable
    public Integer getMaxVersions() {
        return maxVersions;
    }

    /** Returns the age cap of a {@link Kind#MAX_AGE} rule, or {@code null} for other kinds. */
    @Nullable
    public Duration getMaxAge() {
        return maxAge;
    }

    /**
     * Returns the nested rules of a {@link Kind#UNION} or {@link Kind#INTERSECTION} rule, or {@code
     * null} for the leaf kinds.
     */
    @Nullable
    public List<GcRule> getRules() {
        return rules;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GcRule that = (GcRule) o;
        return kind == that.kind
                && Objects.equals(maxVersions, that.maxVersions)
                && Objects.equals(maxAge, that.maxAge)
                && Objects.equals(rules, that.rules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, maxVersions, maxAge, rules);
    }

    @Override
    public String toString() {
        switch (kind) {
            case MAX_VERSIONS:
                return "maxVersions(" + maxVersions + ")";
            case MAX_AGE:
                return "maxAge(" + maxAge + ")";
            default:
                return kind.name().toLowerCase(Locale.ROOT)
                        + rules.stream()
                                .map(GcRule::toString)
                                .collect(Collectors.joining(", ", "(", ")"));
        }
    }
}
