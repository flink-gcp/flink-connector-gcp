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

package io.github.flink.gcp.connector.bigquery.sql;

import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorPackagingITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;

import java.util.List;

/**
 * Asserts the shape of this module's uber-jar. The checks are the shared ones; what is BigQuery's
 * own is the jar, the factory, and the one package root left unrelocated below.
 *
 * <p>The base class checks classes, deliberately. In this tree 172 non-class resources outside
 * {@code META-INF/} keep their original paths (measured 2026-08-06, against the Pub/Sub bundle's
 * 128 on the same measure): the {@code google/**}{@code /*.proto} descriptors from the proto
 * artifacts, the {@code .java} sources {@code jsr305} ships beside its classes, and two that are
 * load-bearing where they are — {@code mozilla/public-suffix-list.txt}, which httpclient reads by
 * literal path, and gax's root-level {@code dependencies.properties}, whose lookup carries no
 * package for shade to rewrite. The proto descriptors are inert for a Java runtime (protobuf reads
 * descriptors compiled into the generated classes, not these files), which is why every surveyed
 * uber-jar leaves them alone. {@code dependencies.properties} is the one with a real, if cosmetic,
 * collision surface: another jar in {@code lib/} shipping one would feed a foreign version string
 * into the relocated gax's {@code x-goog-api-client} header — and with the Pub/Sub uber-jar this is
 * no longer hypothetical, since both bundle gax and whichever loader wins decides.
 *
 * <p>{@link BigQuerySqlConnectorSmokeITCase} is what proves the relocated classes actually work.
 */
class BigQuerySqlConnectorPackagingITCase extends AbstractSqlConnectorPackagingITCase {

    @Override
    protected ShadedJar shadedJar() {
        return UberJar.SHADED;
    }

    @Override
    protected String factoryClass() {
        return UberJar.FACTORY_CLASS;
    }

    @Override
    protected List<String> additionalUnrelocatedPackages() {
        // org/checkerframework/ is annotation-only and exempt for the reason the base class gives
        // for its own five, but it belongs here rather than there: only this tree carries it
        // (google-cloud-bigquery brings checker-compat-qual), and a shared entry the Pub/Sub jar
        // has no classes under would exempt a package that arrives there later.
        return List.of("io/github/flink/gcp/connector/bigquery/", "org/checkerframework/");
    }

    @Override
    protected int minimumBundledArtifacts() {
        return UberJar.MINIMUM_BUNDLED_ARTIFACTS;
    }
}
