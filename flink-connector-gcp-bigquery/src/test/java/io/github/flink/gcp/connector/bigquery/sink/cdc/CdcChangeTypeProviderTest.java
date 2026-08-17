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

package io.github.flink.gcp.connector.bigquery.sink.cdc;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the built-in {@link CdcChangeTypeProvider} implementations. */
class CdcChangeTypeProviderTest {

    @Test
    void upsertOnlyTreatsEveryRecordAsAnUpsert() {
        assertThat(CdcChangeTypeProvider.upsertOnly().getChangeType("any"))
                .isEqualTo(CdcChangeType.UPSERT);
    }

    @Test
    void upsertOnlyCrossesTheJobGraphAsAnEnumRatherThanALambda() throws Exception {
        // A lambda would be bound back by its SerializedLambda synthetic-method name, which the
        // compiler picks; an enum constant is bound by its own name, which this project owns.
        CdcChangeTypeProvider<String> provider = CdcChangeTypeProvider.upsertOnly();

        byte[] serialized = InstantiationUtil.serializeObject(provider);

        assertThat(new String(serialized, StandardCharsets.ISO_8859_1))
                .doesNotContain("SerializedLambda");
        CdcChangeTypeProvider<String> restored =
                InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
        assertThat(restored).isSameAs(provider);
        assertThat(restored.getChangeType("any")).isEqualTo(CdcChangeType.UPSERT);
    }

    @Test
    void upsertOnlyNamesItsFactoryWhenPrinted() {
        assertThat(CdcChangeTypeProvider.upsertOnly())
                .hasToString("CdcChangeTypeProvider.upsertOnly()");
    }
}
