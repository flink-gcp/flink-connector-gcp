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

package io.github.flink.gcp.connector.bigtable.sink.singlerow;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.lang.reflect.Type;
import java.util.Map;

/** Supplies the connector-owned serializer for {@link BigtableRow}. */
@Internal
public final class BigtableRowTypeInfoFactory extends TypeInfoFactory<BigtableRow> {

    private static final TypeInformation<BigtableRow> TYPE_INFORMATION = new RowTypeInformation();

    @Override
    public TypeInformation<BigtableRow> createTypeInfo(
            Type type, Map<String, TypeInformation<?>> genericParameters) {
        return TYPE_INFORMATION;
    }

    private static final class RowTypeInformation extends TypeInformation<BigtableRow> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isBasicType() {
            return false;
        }

        @Override
        public boolean isTupleType() {
            return false;
        }

        @Override
        public int getArity() {
            return 1;
        }

        @Override
        public int getTotalFields() {
            return 1;
        }

        @Override
        public Class<BigtableRow> getTypeClass() {
            return BigtableRow.class;
        }

        @Override
        public boolean isKeyType() {
            return false;
        }

        @Override
        public TypeSerializer<BigtableRow> createSerializer(SerializerConfig serializerConfig) {
            return new BigtableRowSerializer();
        }

        /** Flink 1.20 entry point; Flink 2.x calls the SerializerConfig overload above. */
        @SuppressWarnings("deprecation")
        public TypeSerializer<BigtableRow> createSerializer(ExecutionConfig executionConfig) {
            return new BigtableRowSerializer();
        }

        @Override
        public boolean canEqual(Object other) {
            return other instanceof RowTypeInformation;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof RowTypeInformation;
        }

        @Override
        public int hashCode() {
            return RowTypeInformation.class.hashCode();
        }

        @Override
        public String toString() {
            return "BigtableRow";
        }
    }
}
