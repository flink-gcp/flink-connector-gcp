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

package io.github.flink.gcp.connector.spanner.table.catalog;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import io.github.flink.gcp.connector.spanner.table.catalog.SpannerCatalogClient.NamedTypeKind;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SpannerServiceCatalogClient#kindsOf(FileDescriptorSet)}: the names a column's {@code
 * SPANNER_TYPE} gives a proto bundle's types, nested ones included.
 */
class SpannerServiceCatalogClientTest {

    @Test
    void namesEveryMessageAndEnumByItsFullyQualifiedNameNestedOnesIncluded() {
        FileDescriptorSet files =
                FileDescriptorSet.newBuilder()
                        .addFile(
                                FileDescriptorProto.newBuilder()
                                        .setName("example/events.proto")
                                        .setPackage("example.events")
                                        .addMessageType(
                                                DescriptorProto.newBuilder()
                                                        .setName("Outer")
                                                        .addNestedType(
                                                                DescriptorProto.newBuilder()
                                                                        .setName("Inner")
                                                                        .addEnumType(
                                                                                EnumDescriptorProto
                                                                                        .newBuilder()
                                                                                        .setName(
                                                                                                "Deep")))
                                                        .addEnumType(
                                                                EnumDescriptorProto.newBuilder()
                                                                        .setName("Kind")))
                                        .addEnumType(
                                                EnumDescriptorProto.newBuilder().setName("Status")))
                        .addFile(
                                FileDescriptorProto.newBuilder()
                                        .setName("bare.proto")
                                        .addMessageType(
                                                DescriptorProto.newBuilder().setName("Bare")))
                        .build();

        assertThat(SpannerServiceCatalogClient.kindsOf(files))
                .containsOnly(
                        Map.entry("example.events.Outer", NamedTypeKind.PROTO),
                        Map.entry("example.events.Outer.Inner", NamedTypeKind.PROTO),
                        Map.entry("example.events.Outer.Inner.Deep", NamedTypeKind.ENUM),
                        Map.entry("example.events.Outer.Kind", NamedTypeKind.ENUM),
                        Map.entry("example.events.Status", NamedTypeKind.ENUM),
                        Map.entry("Bare", NamedTypeKind.PROTO));
    }
}
