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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import org.apache.flink.annotation.Internal;

import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.protobuf.Timestamp;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceConfig;

import java.time.Instant;

/**
 * Builds the source's {@link CreateReadSessionRequest}.
 *
 * <p>Split out of the enumerator because it is the whole of the connector's push-down surface and
 * is pure: every builder knob that reaches BigQuery reaches it through this class, and a test of
 * the mapping needs no session, no client and no context.
 */
@Internal
final class ReadSessionRequests {

    private ReadSessionRequests() {}

    /**
     * Builds the request creating the source's read session.
     *
     * <p>The table is a parameter rather than a field read off the configuration: a query source
     * has no table until its query has run, and this is where the two kinds of source become one.
     *
     * @param config the source configuration
     * @param table the table to read — the configured one, or the one a query's result landed in
     * @return the request
     */
    static CreateReadSessionRequest of(BigQuerySourceConfig<?> config, TableDestination table) {
        ReadSession.TableReadOptions.Builder readOptions =
                ReadSession.TableReadOptions.newBuilder()
                        .addAllSelectedFields(config.getSelectedFields());
        if (config.getRowRestriction() != null) {
            readOptions.setRowRestriction(config.getRowRestriction());
        }

        ReadSession.Builder session =
                ReadSession.newBuilder()
                        .setTable(table.toTablePath())
                        .setDataFormat(DataFormat.AVRO)
                        .setReadOptions(readOptions.build());
        Instant snapshotTime = config.getSnapshotTime();
        if (snapshotTime != null) {
            session.setTableModifiers(
                    ReadSession.TableModifiers.newBuilder()
                            .setSnapshotTime(
                                    Timestamp.newBuilder()
                                            .setSeconds(snapshotTime.getEpochSecond())
                                            .setNanos(snapshotTime.getNano())
                                            .build())
                            .build());
        }

        CreateReadSessionRequest.Builder request =
                CreateReadSessionRequest.newBuilder()
                        .setParent("projects/" + config.getParentProject())
                        .setReadSession(session.build());
        // Both knobs are left unset at zero, which is how the API spells "the server decides". Only
        // a set value is written, so an unconfigured source sends neither field.
        if (config.getMaxStreamCount() > 0) {
            request.setMaxStreamCount(config.getMaxStreamCount());
        }
        if (config.getPreferredMinStreamCount() > 0) {
            request.setPreferredMinStreamCount(config.getPreferredMinStreamCount());
        }
        return request.build();
    }
}
