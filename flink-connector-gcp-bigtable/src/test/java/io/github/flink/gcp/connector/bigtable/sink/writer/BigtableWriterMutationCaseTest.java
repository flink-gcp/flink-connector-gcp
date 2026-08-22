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

package io.github.flink.gcp.connector.bigtable.sink.writer;

import com.google.bigtable.v2.Mutation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for what {@link BigtableWriter}'s mutation-case switch is written against. */
class BigtableWriterMutationCaseTest {

    @Test
    void theMutationCasesTheSwitchIsWrittenAgainstAreStillTheOnesTheClientHas() {
        // BigtableWriter.missingFamilies reads the column families an entry names by switching
        // over this client enum, to decide whether a family creation cannot supply is to blame
        // for a NOT_FOUND. A case that switch does not name is left unresolved and read as
        // naming no family, so the entry parks and — if the case did name an undeclared family —
        // fails only once the recovery budget is spent, with a message naming no family at all.
        // Throwing there instead is not open: the same method sees entries whose families are
        // declared and merely not visible yet, and those park and then succeed.
        //
        // The client has grown this enum before: measured with javap, it carries five cases at
        // 1.27.1, 2.14.1 and 2.20.1 and seven at 2.45.1, 2.80.0 and 2.81.0, and both additions —
        // ADD_TO_CELL and MERGE_TO_CELL — name a column family. The growth arrives through a
        // libraries-bom bump, so this assertion is what spends it on that pull request's CI
        // instead of on a job. Add a constant here only together with its arm in
        // missingFamilies, or with a note saying it names none.
        assertThat(Mutation.MutationCase.values())
                .extracting(Enum::name)
                .containsExactlyInAnyOrder(
                        "SET_CELL",
                        "ADD_TO_CELL",
                        "MERGE_TO_CELL",
                        "DELETE_FROM_COLUMN",
                        "DELETE_FROM_FAMILY",
                        "DELETE_FROM_ROW",
                        "MUTATION_NOT_SET");
    }
}
