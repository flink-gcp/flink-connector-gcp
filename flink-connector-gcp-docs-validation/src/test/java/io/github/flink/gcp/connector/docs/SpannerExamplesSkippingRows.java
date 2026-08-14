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

package io.github.flink.gcp.connector.docs;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.docs.SpannerDocumentationTypes.Order;
import io.github.flink.gcp.connector.spanner.source.serializer.SpannerStructDeserializationSchema;

final class SpannerExamplesSkippingRows {

    private SpannerExamplesSkippingRows() {}

    static void build() {
        // tag::spanner-examples-skipping-rows[]
        new SpannerStructDeserializationSchema<Order>() {
            @Override
            public void deserialize(Struct row, Collector<Order> out) {
                // Rows the query could not exclude, filtered before they cost anything downstream.
                if (!row.isNull("Total")) {
                    out.collect(new Order(row.getString("OrderId"), row.getLong("Total")));
                }
            }

            @Override
            public TypeInformation<Order> getProducedType() {
                return TypeInformation.of(Order.class);
            }
        };
        // end::spanner-examples-skipping-rows[]
    }
}
