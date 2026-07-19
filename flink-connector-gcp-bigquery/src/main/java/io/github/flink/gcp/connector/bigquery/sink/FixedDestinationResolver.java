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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.util.Preconditions;

/**
 * A {@link DestinationResolver} returning one fixed destination for every record.
 *
 * <p>A named class (instead of a synthesized lambda) for two reasons: lambda serialization would
 * tie the job graph to fragile {@code SerializedLambda} synthetic-method identity across connector
 * versions, and writers can detect the single-destination case (via {@code instanceof} and {@link
 * #getDestination()}) to skip per-record destination resolution entirely.
 */
@Internal
public final class FixedDestinationResolver implements DestinationResolver<Object> {

    private static final long serialVersionUID = 1L;

    private final TableDestination destination;

    /**
     * Creates a resolver returning the given destination for every record.
     *
     * @param destination the destination table
     */
    public FixedDestinationResolver(TableDestination destination) {
        this.destination = Preconditions.checkNotNull(destination, "destination must not be null");
    }

    /** Returns the fixed destination. */
    public TableDestination getDestination() {
        return destination;
    }

    @Override
    public TableDestination resolve(Object element, SinkWriter.Context context) {
        return destination;
    }
}
