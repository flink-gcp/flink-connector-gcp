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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.InstantiationUtil;

import com.google.cloud.spanner.Dialect;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannerTableNameTest {

    @Test
    void preservesTheLegacyTableValueWhenSchemaIsUnset() {
        SpannerTableName google =
                SpannerTableName.of(null, "legacy.table", Dialect.GOOGLE_STANDARD_SQL);
        SpannerTableName postgres = SpannerTableName.of(null, "Legacy.Table", Dialect.POSTGRESQL);

        assertThat(google.apiName()).isEqualTo("legacy.table");
        assertThat(google.schema()).isEmpty();
        assertThat(google.table()).isEqualTo("legacy.table");
        assertThat(google.accessPath("legacy.index", "scan.index").apiName())
                .isEqualTo("legacy.index");
        assertThat(postgres.apiName()).isEqualTo("Legacy.Table");
        assertThat(postgres.schema()).isEqualTo("public");
        assertThat(postgres.table()).isEqualTo("Legacy.Table");
    }

    @Test
    void foldsUnquotedPostgresqlNamesAndQualifiesTheAccessPath() {
        SpannerTableName table = SpannerTableName.of("Analytics", "Orders", Dialect.POSTGRESQL);

        assertThat(table.schema()).isEqualTo("analytics");
        assertThat(table.table()).isEqualTo("orders");
        assertThat(table.apiName()).isEqualTo("analytics.orders");
        assertThat(table.accessPath("ByScore", "scan.index").apiName())
                .isEqualTo("analytics.byscore");
        assertThat(table.accessPath("ByScore", "scan.index").catalogName()).isEqualTo("byscore");
    }

    @Test
    void preservesQuotedPostgresqlNames() {
        SpannerTableName table =
                SpannerTableName.of("\"Sales\"", "\"Order\"\"Items\"", Dialect.POSTGRESQL);

        assertThat(table.schema()).isEqualTo("Sales");
        assertThat(table.table()).isEqualTo("Order\"Items");
        assertThat(table.apiName()).isEqualTo("Sales.Order\"Items");
        assertThat(table.accessPath("\"ByScore\"", "scan.index").apiName())
                .isEqualTo("Sales.ByScore");
    }

    @Test
    void decodesCanonicalGoogleSqlQuotedEscapesForTheCatalog() {
        SpannerTableName table =
                SpannerTableName.of(
                        "`sales\\\\west`", "`order\\`items`", Dialect.GOOGLE_STANDARD_SQL);

        assertThat(table.schema()).isEqualTo("sales\\west");
        assertThat(table.table()).isEqualTo("order`items");
        assertThat(table.apiName()).isEqualTo("sales\\west.order`items");
    }

    @Test
    void keysDecodedNativeApiNamesWithoutSqlCaseFolding() {
        assertThat(SpannerTableName.nativeApiKey("Sales.Orders", Dialect.GOOGLE_STANDARD_SQL))
                .isEqualTo(
                        SpannerTableName.catalogKey(
                                "Sales", "Orders", Dialect.GOOGLE_STANDARD_SQL));
        assertThat(SpannerTableName.nativeApiKey("Sales.Orders", Dialect.POSTGRESQL))
                .isEqualTo(SpannerTableName.catalogKey("Sales", "Orders", Dialect.POSTGRESQL));
    }

    @Test
    void matchesLegacyMultipartNativeNamesWithoutReinterpretingTheConfiguredTable() {
        SpannerTableName google =
                SpannerTableName.of(null, "analytics.people", Dialect.GOOGLE_STANDARD_SQL);
        SpannerTableName postgres =
                SpannerTableName.of(null, "Analytics.People", Dialect.POSTGRESQL);

        assertThat(google.matchesNativeApiName("analytics.people")).isTrue();
        assertThat(google.matchesNativeApiName("other.people")).isFalse();
        assertThat(postgres.matchesNativeApiName("Analytics.People")).isTrue();
        assertThat(postgres.matchesNativeApiName("analytics.people")).isFalse();
    }

    @Test
    void matchesExplicitNamesByDialectAwareSchemaAndTableComponents() {
        SpannerTableName google =
                SpannerTableName.of("Analytics", "People", Dialect.GOOGLE_STANDARD_SQL);
        SpannerTableName postgres =
                SpannerTableName.of("\"Analytics\"", "\"People\"", Dialect.POSTGRESQL);

        assertThat(google.matchesNativeApiName("analytics.people")).isTrue();
        assertThat(google.matchesNativeApiName("other.people")).isFalse();
        assertThat(google.matchesNativeApiName("analytics.other")).isFalse();
        assertThat(postgres.matchesNativeApiName("Analytics.People")).isTrue();
        assertThat(postgres.matchesNativeApiName("analytics.people")).isFalse();
        assertThat(postgres.matchesNativeApiName("Other.People")).isFalse();
    }

    @Test
    void rejectsWhitespaceThatTrimDoesNotStrip() {
        // U+2028 is the value that tells the two blank idioms apart: Character.isWhitespace calls
        // it whitespace, and String.trim() leaves it alone because it sits above U+0020. The ASCII
        // assertions elsewhere in this class pass under either idiom; these two fail if the check
        // each of them reaches returns to trim().isEmpty().
        //
        // The second one passes a null schema deliberately. accessPath's own blank check runs only
        // on the unqualified branch: with a schema configured it delegates to SpannerIdentifier
        // instead, which is how an earlier revision of this test pinned SpannerIdentifier twice and
        // SpannerTableName's own check not at all.
        assertThatThrownBy(
                        () -> SpannerTableName.of("\u2028", "orders", Dialect.GOOGLE_STANDARD_SQL))
                .as("SpannerIdentifier, reached through a configured schema")
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("schema");
        assertThatThrownBy(
                        () ->
                                SpannerTableName.of(null, "orders", Dialect.POSTGRESQL)
                                        .accessPath("\u2028", "scan.index"))
                .as("accessPath's own check, on the unqualified branch")
                .isInstanceOf(ValidationException.class)
                .hasMessage("scan.index must not be blank.");
    }

    @Test
    void rejectsABlankNativeApiTableName() {
        // nativeApiKey's blank branch had no test at all; it is the mutation-side entry point.
        // The third value is U+2028, which trim() leaves in place and only isBlank() rejects.
        assertThatThrownBy(() -> SpannerTableName.nativeApiKey(null, Dialect.GOOGLE_STANDARD_SQL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The native API table name is blank.");
        assertThatThrownBy(() -> SpannerTableName.nativeApiKey("  ", Dialect.POSTGRESQL))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The native API table name is blank.");
        assertThatThrownBy(
                        () -> SpannerTableName.nativeApiKey("\u2028", Dialect.GOOGLE_STANDARD_SQL))
                .as("U+2028 is blank to isBlank() but survives trim()")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("The native API table name is blank.");
    }

    @Test
    void rejectsBlankMultipartAndDialectMismatchedIdentifiers() {
        assertThatThrownBy(() -> SpannerTableName.of(" ", "orders", Dialect.GOOGLE_STANDARD_SQL))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("schema");
        assertThatThrownBy(() -> SpannerTableName.of("sales", " ", Dialect.POSTGRESQL))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("table");
        assertThatThrownBy(
                        () ->
                                SpannerTableName.of(
                                        "sales", "sales.orders", Dialect.GOOGLE_STANDARD_SQL))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("table");
        assertThatThrownBy(
                        () ->
                                SpannerTableName.of("sales", "orders", Dialect.GOOGLE_STANDARD_SQL)
                                        .accessPath("\"ByScore\"", "scan.index"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("scan.index");
        assertThatThrownBy(
                        () ->
                                SpannerTableName.of("sales", "orders", Dialect.POSTGRESQL)
                                        .accessPath(" ", "scan.index"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("scan.index");
        assertThatThrownBy(
                        () ->
                                SpannerTableName.of(
                                        "`sales\\q`", "orders", Dialect.GOOGLE_STANDARD_SQL))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("schema");
        assertThatThrownBy(
                        () ->
                                SpannerTableName.of(
                                        "\"sales\"", "\"order\"item\"", Dialect.POSTGRESQL))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("table");
    }

    @Test
    void leavesReservedWordValidationToSpanner() {
        assertThat(SpannerTableName.of("SELECT", "orders", Dialect.GOOGLE_STANDARD_SQL).apiName())
                .isEqualTo("SELECT.orders");
        assertThat(SpannerTableName.of("GROUP", "orders", Dialect.POSTGRESQL).apiName())
                .isEqualTo("group.orders");
        assertThat(SpannerTableName.of("`SELECT`", "GROUP", Dialect.GOOGLE_STANDARD_SQL).apiName())
                .isEqualTo("SELECT.GROUP");
        assertThat(SpannerTableName.of("\"GROUP\"", "SELECT", Dialect.POSTGRESQL).apiName())
                .isEqualTo("GROUP.select");
    }

    @Test
    void leavesPostgresqlObjectNameLimitsToSpanner() {
        assertThat(SpannerTableName.of("_analytics", "orders", Dialect.POSTGRESQL).schema())
                .isEqualTo("_analytics");
        assertThat(SpannerTableName.of("a".repeat(64), "orders", Dialect.POSTGRESQL).schema())
                .isEqualTo("a".repeat(64));
    }

    @Test
    void leavesGoogleSqlObjectNameLimitsToSpanner() {
        assertThat(
                        SpannerTableName.of("_analytics", "orders", Dialect.GOOGLE_STANDARD_SQL)
                                .schema())
                .isEqualTo("_analytics");
        assertThat(
                        SpannerTableName.of("a".repeat(129), "orders", Dialect.GOOGLE_STANDARD_SQL)
                                .schema())
                .isEqualTo("a".repeat(129));
        assertThat(
                        SpannerTableName.of("`sales west`", "orders", Dialect.GOOGLE_STANDARD_SQL)
                                .schema())
                .isEqualTo("sales west");
    }

    @Test
    void isSerializableWithItsResolvedAccessPath() throws Exception {
        SpannerTableName table = SpannerTableName.of("\"Sales\"", "\"Orders\"", Dialect.POSTGRESQL);

        assertThat(InstantiationUtil.clone(table)).isEqualTo(table);
        assertThat(InstantiationUtil.clone(table.accessPath("\"ByScore\"", "scan.index")))
                .isEqualTo(table.accessPath("\"ByScore\"", "scan.index"));
    }
}
