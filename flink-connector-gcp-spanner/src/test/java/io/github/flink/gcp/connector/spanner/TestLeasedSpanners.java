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

package io.github.flink.gcp.connector.spanner;

import com.google.cloud.spanner.Spanner;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;

/**
 * Pairs a test double of the Spanner handle with a lease of its own, for tests in other packages
 * that hand a component a {@link LeasedSpanner}. The caller checks the lease is released.
 */
public final class TestLeasedSpanners {

    private TestLeasedSpanners() {}

    /**
     * Pairs the handle with a newly opened lease.
     *
     * @param spanner the handle
     * @return the pair, which releases the lease once the handle closes
     */
    public static LeasedSpanner leased(Spanner spanner) {
        return new LeasedSpanner(spanner, GrpcEventLoops.acquire());
    }
}
