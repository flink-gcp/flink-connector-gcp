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

package io.github.flink.gcp.connector.firestore.sql;

import io.github.flink.gcp.connector.testutils.sql.AbstractSqlConnectorPackagingITCase;
import io.github.flink.gcp.connector.testutils.sql.ShadedJar;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Asserts the shape of this module's uber-jar. The checks are the shared ones; what is this
 * module's own is the jar, the factory, and the package roots left unrelocated below: both of the
 * connector's roots, Firestore Native mode and Datastore mode, are this jar's public surface
 * (ADR-0170), and {@code org/checkerframework/} is annotation-only, exempt for the reason the base
 * class gives the others.
 *
 * <p>{@link FirestoreSqlConnectorSmokeITCase} is what proves the relocated classes actually work.
 */
class FirestoreSqlConnectorPackagingITCase extends AbstractSqlConnectorPackagingITCase {

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
        return List.of(
                "io/github/flink/gcp/connector/firestore/",
                "io/github/flink/gcp/connector/datastore/",
                "org/checkerframework/");
    }

    @Override
    protected int minimumBundledArtifacts() {
        return UberJar.MINIMUM_BUNDLED_ARTIFACTS;
    }

    /**
     * The shared check names one factory; the jar's SPI file carries a second, Datastore mode's,
     * which the shade plugin must keep beside the first rather than overwrite.
     */
    @Test
    void sqlCanDiscoverTheDatastoreFactoryToo() throws Exception {
        try (JarFile jar = new JarFile(UberJar.SHADED.path().toFile())) {
            JarEntry services =
                    jar.getJarEntry("META-INF/services/org.apache.flink.table.factories.Factory");
            assertThat(services).isNotNull();
            assertThat(
                            new String(
                                    jar.getInputStream(services).readAllBytes(),
                                    StandardCharsets.UTF_8))
                    .contains(UberJar.FACTORY_CLASS)
                    .contains(UberJar.DATASTORE_FACTORY_CLASS);
        }
    }
}
