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

package io.github.flink.gcp.connector.tier3.spanner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.UUID;

/**
 * Names each delivery of a source by its phase, its task instance and its position in that
 * instance. Both relayed tables key a row by its source key and this name, so a record the source
 * delivers again, after a restore or within one instance, adds a row, and a key's rows count its
 * deliveries.
 */
@Internal
final class Delivery extends RichMapFunction<Tuple2<String, Long>, Tuple3<String, String, Long>> {
    private static final long serialVersionUID = 1L;

    /** What both sources emit: a key and the value its delivery carries. */
    static final TypeInformation<Tuple2<String, Long>> RECORD =
            Types.TUPLE(Types.STRING, Types.LONG);

    private final String phase;
    private transient String instance;
    private transient long deliveries;

    Delivery(String phase) {
        this.phase = phase;
    }

    @Override
    public void open(OpenContext context) {
        instance = phase + "/" + UUID.randomUUID() + "/";
        deliveries = 0;
    }

    @Override
    public Tuple3<String, String, Long> map(Tuple2<String, Long> record) {
        return Tuple3.of(record.f0, instance + deliveries++, record.f1);
    }
}
