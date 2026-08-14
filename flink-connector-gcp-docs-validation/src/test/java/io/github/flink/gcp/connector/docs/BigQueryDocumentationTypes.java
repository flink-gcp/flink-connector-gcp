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

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.sink.serializer.BigQueryProtoSerializer;

import java.time.Instant;

final class BigQueryDocumentationTypes {

    private BigQueryDocumentationTypes() {}

    static final class OrderEvent {

        private final Instant createdAt;

        private OrderEvent(Instant createdAt) {
            this.createdAt = createdAt;
        }

        Instant createdAt() {
            return createdAt;
        }
    }

    interface MyEvent {

        String tableName();

        String uuid();

        Instant timestamp();
    }

    interface Order {}

    interface MyMessage extends Message {}

    static final class MyAnnotations {

        private MyAnnotations() {}

        static GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, Boolean> json;

        static GeneratedMessage.GeneratedExtension<DescriptorProtos.FieldOptions, Boolean>
                geography;
    }

    static final class MyEventProtoSerializer extends BigQueryProtoSerializer<MyEvent> {

        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.getDefaultInstance();
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(MyEvent element) {
            return Empty.getDefaultInstance().toByteString();
        }
    }

    static final class MyOrderProtoSerializer extends BigQueryProtoSerializer<Order> {

        private static final long serialVersionUID = 1L;

        @Override
        public TableSchema getTableSchema(TableDestination destination) {
            return TableSchema.getDefaultInstance();
        }

        @Override
        public Descriptors.Descriptor getDescriptor(TableDestination destination) {
            return Empty.getDescriptor();
        }

        @Override
        public ByteString serialize(Order element) {
            return Empty.getDefaultInstance().toByteString();
        }
    }
}
