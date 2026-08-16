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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigtable.TableDestination;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Asks the service where a table's sections begin.
 *
 * <p>The seam the split enumerator plans against, so that the planning protocol can be tested
 * without a client and without a billed instance. The emulator's {@code SampleRowKeys} answers with
 * the table's final key plus a scattering of random ones, which is enough to prove the wiring and
 * not nearly enough to prove a plan.
 *
 * <p>{@link Serializable} because the source configuration this travels in goes into the job graph.
 * An implementation creates its client on first use rather than in its constructor, so that
 * building a job needs no credentials.
 *
 * <p>{@code close()} releases whatever the implementation built. The enumerator owns the sampler
 * and closes it once, so nothing here has to tolerate being closed twice by different owners.
 */
@Internal
public interface RowKeySampler extends Serializable, AutoCloseable {

    /**
     * Returns the sampled section boundaries of a table.
     *
     * <p>Called once per job from the enumerator's asynchronous planning step, which is why a
     * blocking implementation is fine. The samples may arrive in any order and may include the
     * service's empty "end of table" key; interpreting them is the planner's job.
     *
     * @param table the table to sample
     * @return the samples, possibly empty when the table is small enough to have one section
     * @throws IOException if the call fails
     */
    List<RowKeySample> sample(TableDestination table) throws IOException;

    @Override
    void close() throws IOException;
}
