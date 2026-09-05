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

package io.github.flink.gcp.connector.bigtable.sink.conditional;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableConditionalApiTest {
    private static final TableDestination TABLE = TableDestination.of("p", "i", "t");

    @Test
    void sinkAndFunctionCrossTheJobGraphWithConnectorOwnedConfiguration() throws Exception {
        BigtableConditionalSink<String> sink =
                BigtableConditionalSink.<String>builder()
                        .table(TABLE)
                        .serializer(new Schema())
                        .emulatorEndpoint(EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"))
                        .build();
        assertThat(InstantiationUtil.clone(sink, getClass().getClassLoader())).isNotSameAs(sink);
        ConditionalFunction<String> function = async().function();
        assertThat(InstantiationUtil.clone(function, getClass().getClassLoader()))
                .isNotSameAs(function);
    }

    @Test
    void buildersRejectAKeyFileWithAnEmulatorBeforeReadingCredentials() {
        EmulatorEndpoint endpoint = EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint");
        assertThatThrownBy(
                        () ->
                                BigtableConditionalSink.<String>builder()
                                        .table(TABLE)
                                        .serializer(new Schema())
                                        .serviceAccountKeyFile("nonexistent.json")
                                        .emulatorEndpoint(endpoint)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorEndpoint(...)");
        assertThatThrownBy(
                        () ->
                                BigtableConditionalAsync.<String>builder()
                                        .table(TABLE)
                                        .serializer(new Schema())
                                        .emulatorEndpoint(endpoint)
                                        .serviceAccountKeyFile("nonexistent.json")
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("serviceAccountKeyFile(...)")
                .hasMessageContaining("emulatorEndpoint(...)");
    }

    @Test
    void helperBuildsAnExplicitTupleTypeWithoutGenericResultSerialization() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ((org.apache.flink.api.common.serialization.SerializerConfigImpl)
                        env.getConfig().getSerializerConfig())
                .setGenericTypes(false);
        DataStream<String> input = env.fromData("a", "b");
        DataStream<Tuple2<String, ConditionalResult>> ordered =
                async().orderedWait(input, Duration.ofSeconds(21));
        DataStream<Tuple2<String, ConditionalResult>> unordered =
                async().unorderedWait(input, Duration.ofSeconds(21));
        assertThat(ordered.getType()).isEqualTo(unordered.getType());
        TupleSerializer<?> serializer =
                (TupleSerializer<?>)
                        ordered.getType().createSerializer(env.getConfig().getSerializerConfig());
        assertThat(serializer.getFieldSerializers()[1])
                .isInstanceOf(ConditionalResultSerializer.class);
        assertThat(env.getStreamGraph()).isNotNull();
    }

    @Test
    void helperRejectsTimeoutsAtOrBelowTheSdkDeadlineAndUnsafeConversions() {
        DataStream<String> input =
                StreamExecutionEnvironment.getExecutionEnvironment().fromData("a");
        assertThatThrownBy(() -> async().orderedWait(input, Duration.ofSeconds(20)))
                .hasMessageContaining("greater than");
        assertThatThrownBy(() -> async().unorderedWait(input, Duration.ofSeconds(19)))
                .hasMessageContaining("greater than");
        assertThatThrownBy(() -> async().orderedWait(input, Duration.ofSeconds(20).plusNanos(1)))
                .hasMessageContaining("milliseconds");
        assertThat(async().unorderedWait(input, Duration.ofSeconds(20).plusMillis(1))).isNotNull();
        assertThatThrownBy(() -> async().unorderedWait(input, Duration.ofSeconds(Long.MAX_VALUE)))
                .hasMessageContaining("timeout");
    }

    private static BigtableConditionalAsync<String> async() {
        return BigtableConditionalAsync.<String>builder()
                .table(TABLE)
                .serializer(new Schema())
                .emulatorEndpoint(EmulatorEndpoint.parse("localhost:1", "emulatorEndpoint"))
                .build();
    }

    private static final class Schema implements ConditionalSerializationSchema<String> {
        private static final long serialVersionUID = 1L;

        @Override
        public ConditionalRequest serialize(
                String input, org.apache.flink.api.connector.sink2.SinkWriter.Context context) {
            return ConditionalRequest.of(
                    ByteString.copyFromUtf8(input),
                    ConditionalFilter.rowExists(),
                    List.of(),
                    List.of(
                            ConditionalMutation.setCell(
                                    "cf", ByteString.EMPTY, 1000, ByteString.EMPTY)));
        }
    }
}
