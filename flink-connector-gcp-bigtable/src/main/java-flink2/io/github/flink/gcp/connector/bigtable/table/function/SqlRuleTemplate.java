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

package io.github.flink.gcp.connector.bigtable.table.function;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.readmodifywrite.ReadModifyWriteRule;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/** One append or increment, preserving repeated cells in the enclosing rule list. */
@Internal
final class SqlRuleTemplate implements Serializable {
    private static final long serialVersionUID = 1L;
    final Cell cell;
    final boolean increment;
    private final SqlValueBinding value;

    private SqlRuleTemplate(Cell cell, boolean increment, SqlValueBinding value) {
        this.cell = cell;
        this.increment = increment;
        this.value = value;
    }

    static SqlRuleTemplate parse(SqlSettingMap settings, List<DataType> arguments) {
        String operation = settings.required("operation");
        if (!operation.equals("append") && !operation.equals("increment")) {
            throw settings.error("operation", "must be append or increment");
        }
        Cell cell = new Cell(settings.nonblank("family"), SqlValueBinding.qualifier(settings));
        SqlValueBinding value = SqlValueBinding.value(settings, arguments);
        boolean increment = operation.equals("increment");
        if (increment) {
            value.requireInteger();
        } else {
            value.requireAppend();
        }
        settings.finish();
        return new SqlRuleTemplate(cell, increment, value);
    }

    ReadModifyWriteRule instantiate(RowData input) {
        return increment
                ? ReadModifyWriteRule.increment(
                        cell.family(), cell.qualifier(), value.number(input))
                : ReadModifyWriteRule.append(
                        cell.family(), cell.qualifier(), value.appendBytes(input));
    }

    /** Binary cell identity, independent of the service's result ordering. */
    @Internal
    static final class Cell implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String family;
        private final ByteString qualifier;

        Cell(String family, ByteString qualifier) {
            this.family = family;
            this.qualifier = qualifier;
        }

        String family() {
            return family;
        }

        ByteString qualifier() {
            return qualifier;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Cell)) {
                return false;
            }
            Cell cell = (Cell) other;
            return family.equals(cell.family) && qualifier.equals(cell.qualifier);
        }

        @Override
        public int hashCode() {
            return Objects.hash(family, qualifier);
        }
    }
}
