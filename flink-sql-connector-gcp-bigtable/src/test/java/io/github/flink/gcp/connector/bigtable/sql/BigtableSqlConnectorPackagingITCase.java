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

package io.github.flink.gcp.connector.bigtable.sql;

import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorPackagingITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;

import java.util.List;

/**
 * Asserts the shape of this module's uber-jar. The checks are the shared ones; what is Bigtable's
 * own is the jar, the factory, and the two package roots left unrelocated below.
 *
 * <p>The base class checks classes, deliberately. In this tree around 160 non-class resources keep
 * their original paths: the {@code google/**}{@code /*.proto} and {@code grpc/**}{@code /*.proto}
 * descriptors (inert for a Java runtime — protobuf reads descriptors compiled into the generated
 * classes, not these files), the {@code .java} sources {@code jsr305} ships beside its classes,
 * {@code conscrypt.properties} under the deliberately unrelocated {@code org/conscrypt/}, and three
 * that are load-bearing where they are. Two are the sibling jars' pair — {@code
 * mozilla/public-suffix-list.txt}, which httpclient reads by literal path, and gax's root-level
 * {@code dependencies.properties}, whose lookup carries no package for shade to rewrite — and the
 * third is this tree's own: {@code bigtable-default-client-config.textproto}, which the Bigtable
 * client loads by a root-level literal path the same way. Another jar in {@code lib/} shipping
 * either root-level file would shadow one copy, cosmetically for the gax version header and
 * materially for the client-config defaults.
 *
 * <p>{@link BigtableSqlConnectorSmokeITCase} is what proves the relocated classes actually work.
 */
class BigtableSqlConnectorPackagingITCase extends AbstractSqlConnectorPackagingITCase {

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
        // for its own five. It sits here rather than there for the reason the BigQuery module
        // gives for its identical entry: the base list is the *intersection*, and the Pub/Sub tree
        // carries no checker-qual at all - measured, `dependency:tree -Dscope=runtime` finds one
        // in this tree and none in that one - so a shared entry would exempt a package that
        // arrives there later and should have been relocated.
        return List.of("io/github/flink/gcp/connector/bigtable/", "org/checkerframework/");
    }

    @Override
    protected int minimumBundledArtifacts() {
        return UberJar.MINIMUM_BUNDLED_ARTIFACTS;
    }
}
