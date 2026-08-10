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

package io.github.flink.gcp.connector.bigquery.source.query;

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import java.io.IOException;
import java.io.Serializable;

/**
 * Runs the source's query and says which table its result landed in.
 *
 * <p>The Storage Read API reads storage, so it cannot read a view or the result of a query
 * directly. Everything the query path adds is on this side of the seam: the read session, the
 * splits and the readers are handed a table either way.
 *
 * <p>Abstracts the REST client for the same reason {@code ReadSessionCreator} abstracts the gRPC
 * one — {@code BigQuery} cannot be subclassed usefully and this repository writes no mocks, so a
 * seam is the only way to script a query in a unit test.
 *
 * <p>Serializable because it is held by the source's configuration and therefore travels in the job
 * graph; implementations create their client state on first use, not in their constructor.
 *
 * <p><b>Not {@code AutoCloseable}</b>, unlike the two seams on the read path. Those wrap {@code
 * BigQueryReadClient}, which owns a gRPC channel and an executor that leak if nothing releases
 * them; this one wraps the REST client, and {@code com.google.cloud.bigquery.BigQuery} extends
 * {@code com.google.cloud.Service} and nothing else — there is no {@code close} to call (verified
 * against google-cloud-bigquery 2.68.0, the version {@code libraries-bom} resolves). {@code
 * TableAdmin} on the sink side holds the same client and is not closeable for the same reason. A
 * closeable seam here would be one the enumerator has to compose into its single planner for no
 * released resource.
 */
@Internal
public interface QueryRunner extends Serializable {

    /**
     * Runs the query and returns the table holding its result.
     *
     * <p>Called once per job, from the enumerator's planning call, and never again after a restore:
     * a restored enumerator adopts the read session the first one created.
     *
     * <p>A specification carrying a {@link QueryJobIdentity} asks for the job to be submitted under
     * that identity's deterministic id, and for a previous attempt's job found under it — still
     * running, or done without an error — to be reused rather than run again. Without one the id is
     * random and nothing is ever reused.
     *
     * @param spec the query to run
     * @return the table the result landed in, and whether a previous attempt's job was reused
     * @throws IOException if the job cannot be submitted, fails, or reports no result table
     */
    QueryResult run(QuerySpec spec) throws IOException;

    /**
     * Returns whether the given name is a view rather than a table.
     *
     * <p>Lives here because it is the same REST client, and because what it answers {@code true} to
     * becomes a {@link #run} call: a view is read by materializing it.
     *
     * <p><b>Called only when the source opted into materializing views</b>, and once per job. That
     * is the whole reason it is opt-in: a source reading an ordinary table must not pay a metadata
     * round trip to discover it is an ordinary table, and the read path deliberately makes no REST
     * call at all.
     *
     * @param table the name the source was pointed at
     * @return whether it is a logical or materialized view
     * @throws IOException if the lookup fails, or if nothing exists under that name
     */
    boolean isView(TableDestination table) throws IOException;
}
