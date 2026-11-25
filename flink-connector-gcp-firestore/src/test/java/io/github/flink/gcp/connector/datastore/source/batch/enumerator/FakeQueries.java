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

package io.github.flink.gcp.connector.datastore.source.batch.enumerator;

import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** The planner's queries, answered as the test scripts them, with every request recorded. */
final class FakeQueries implements ClientQueryPlanner.Queries {

    private final Function<RunQueryRequest, RunQueryResponse> answer;
    private final List<RunQueryRequest> requests = new ArrayList<>();

    FakeQueries(Function<RunQueryRequest, RunQueryResponse> answer) {
        this.answer = answer;
    }

    List<RunQueryRequest> requests() {
        return requests;
    }

    @Override
    public RunQueryResponse run(RunQueryRequest request) {
        requests.add(request);
        return answer.apply(request);
    }
}
