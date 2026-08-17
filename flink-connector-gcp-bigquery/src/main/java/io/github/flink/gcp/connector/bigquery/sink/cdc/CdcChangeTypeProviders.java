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

import org.apache.flink.annotation.Internal;

/**
 * The built-in {@link CdcChangeTypeProvider} implementations.
 *
 * <p>Named types (instead of synthesized lambdas) so that nothing the connector itself puts in the
 * job graph depends on {@code SerializedLambda} synthetic-method identity, which is a compiler
 * detail rather than a versioned API. An enum serializes as its own constant name, the way {@code
 * FailureHandlers} keeps the built-in failure policies.
 */
@Internal
final class CdcChangeTypeProviders {

    private CdcChangeTypeProviders() {}

    /** Treats every record as an upsert. */
    enum UpsertOnly implements CdcChangeTypeProvider<Object> {
        INSTANCE;

        @Override
        public CdcChangeType getChangeType(Object element) {
            return CdcChangeType.UPSERT;
        }

        @Override
        public String toString() {
            return "CdcChangeTypeProvider.upsertOnly()";
        }
    }
}
