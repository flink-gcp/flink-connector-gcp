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

package io.github.flink.gcp.connector.firestore.sink.writer;

/** Builds the sink's retry policy from outside its package, for tests elsewhere. */
public final class TestRetryPolicies {

    private TestRetryPolicies() {}

    /**
     * Returns a policy that makes one attempt per write and observes nothing.
     *
     * @return the policy
     */
    public static BulkWriterRetryPolicy singleAttempt() {
        return new BulkWriterRetryPolicy(
                1,
                new BulkWriterRetryPolicy.Observer() {
                    @Override
                    public void attemptFailed() {}

                    @Override
                    public void retrying() {}
                });
    }
}
