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

package io.github.flink.gcp.connector.testutils.sql;

import org.apache.flink.annotation.Internal;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The precondition every {@code flink-sql-connector-gcp-*} smoke test rests on: that the connector
 * it just exercised came out of the uber-jar.
 *
 * <p>One assertion is a thin base class, and it is the right one to share — a subclass's whole
 * point is to run a real job through relocated classes, and if this precondition breaks, every
 * other assertion in it is about the reactor's unshaded code and still passes. What each subclass
 * adds is a service and a job; what it must not have to remember is this.
 */
@Internal
public abstract class AbstractSqlConnectorSmokeITCase {

    /** The jar the connector must have been loaded from. */
    protected abstract ShadedJar shadedJar();

    /** The {@code DynamicTableFactory} the DDL below reaches through the SPI file. */
    protected abstract String factoryClass();

    @Test
    void theConnectorUnderTestComesFromTheShadedJar() throws Exception {
        Class<?> factory = Class.forName(factoryClass());
        Path loadedFrom =
                Path.of(factory.getProtectionDomain().getCodeSource().getLocation().toURI());

        assertThat(loadedFrom)
                .as(
                        "the surefire classpath surgery in this module's pom must put the uber-jar"
                                + " in front of the reactor's unshaded classes, or every other"
                                + " assertion here is about the wrong code")
                .isEqualTo(shadedJar().path().toAbsolutePath());
    }
}
