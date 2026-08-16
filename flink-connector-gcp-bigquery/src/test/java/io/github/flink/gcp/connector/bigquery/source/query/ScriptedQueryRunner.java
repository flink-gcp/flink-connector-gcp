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

package io.github.flink.gcp.connector.bigquery.source.query;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link QueryRunner} answering with a canned result table, recording what it was asked and
 * counting the calls.
 *
 * <p>The counter is what a test asserts a query ran once — or not at all, which is the whole point
 * of the restore path.
 */
public final class ScriptedQueryRunner implements QueryRunner {

    private static final long serialVersionUID = 1L;

    private final TableDestination result;
    @Nullable private final IOException failure;
    private boolean reattached;

    private final AtomicInteger runs = new AtomicInteger();
    private final AtomicInteger viewChecks = new AtomicInteger();
    private final AtomicReference<QuerySpec> lastSpec = new AtomicReference<>();

    /** What {@link #isView} answers; a test reading a plain table sets it to {@code false}. */
    private boolean view = true;

    @Nullable private IOException viewCheckFailure;

    private ScriptedQueryRunner(TableDestination result, @Nullable IOException failure) {
        this.result = result;
        this.failure = failure;
    }

    /** A runner answering with the given table. */
    public static ScriptedQueryRunner answering(TableDestination result) {
        return new ScriptedQueryRunner(result, null);
    }

    /** A runner that fails every call. */
    public static ScriptedQueryRunner failing(IOException failure) {
        return new ScriptedQueryRunner(TableDestination.of("p", "d", "unused"), failure);
    }

    /** Makes {@link #run} report the answered table as a reused previous attempt's job. */
    public ScriptedQueryRunner reattaching() {
        this.reattached = true;
        return this;
    }

    @Override
    public QueryResult run(QuerySpec spec) throws IOException {
        runs.incrementAndGet();
        lastSpec.set(spec);
        if (failure != null) {
            throw failure;
        }
        return new QueryResult(result, reattached);
    }

    @Override
    public boolean isView(TableDestination table) throws IOException {
        viewChecks.incrementAndGet();
        if (viewCheckFailure != null) {
            throw viewCheckFailure;
        }
        return view;
    }

    /** Makes {@link #isView} answer {@code false}, as it does for an ordinary table. */
    public ScriptedQueryRunner answeringNotAView() {
        this.view = false;
        return this;
    }

    /** Makes {@link #isView} fail. */
    public ScriptedQueryRunner failingTheViewCheck(IOException failure) {
        this.viewCheckFailure = failure;
        return this;
    }

    /** Returns how many times a query was run. */
    public int runs() {
        return runs.get();
    }

    /** Returns how many times the source asked whether its table was a view. */
    public int viewChecks() {
        return viewChecks.get();
    }

    /** Returns the specification of the last query run, or {@code null} if none was. */
    @Nullable
    public QuerySpec lastSpec() {
        return lastSpec.get();
    }
}
