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

package io.github.flink.gcp.connector.spanner.sql;

import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorPackagingITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;

import java.util.List;

/**
 * Asserts the shape of this module's uber-jar. The checks are the shared ones; what is Spanner's
 * own is the jar, the factory, and the two package roots left unrelocated below.
 *
 * <p>The base class checks classes, deliberately. In this tree around 160 non-class resources keep
 * their original paths: the {@code google/**}{@code /*.proto} and {@code grpc/**}{@code /*.proto}
 * descriptors (inert for a Java runtime — protobuf reads descriptors compiled into the generated
 * classes, not these files), the {@code .java} sources {@code jsr305} ships beside its classes,
 * {@code conscrypt.properties} under the deliberately unrelocated {@code org/conscrypt/}, and three
 * that are load-bearing where they are. The sibling jars carry the same pair — {@code
 * mozilla/public-suffix-list.txt}, which httpclient reads by literal path, and gax's root-level
 * {@code dependencies.properties}, whose lookup carries no package for shade to rewrite. Another
 * jar in {@code lib/} shipping either root-level file would shadow one copy, cosmetically for the
 * gax version header and materially for the public-suffix data.
 *
 * <p>{@link SpannerSqlConnectorSmokeITCase} is what proves the relocated classes actually work.
 */
class SpannerSqlConnectorPackagingITCase extends AbstractSqlConnectorPackagingITCase {

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
        return List.of("io/github/flink/gcp/connector/spanner/");
    }

    @Override
    protected int minimumBundledArtifacts() {
        return UberJar.MINIMUM_BUNDLED_ARTIFACTS;
    }
}
