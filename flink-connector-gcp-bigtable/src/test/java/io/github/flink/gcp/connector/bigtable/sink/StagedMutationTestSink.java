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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.RowFilter;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A test-only staged protocol, driven by Flink's real sink operators. The fake service evaluates
 * the emitted marker predicate and atomically applies the exercised SetCell/AddToCell shapes; it
 * assumes Bigtable's documented row atomicity and does not establish service behavior.
 */
final class StagedMutationTestSink
        implements CrossVersionSink<StagedMutationTestSink.Input>,
                SupportsCommitter<CheckAndMutateRowRequest> {
    private static final long serialVersionUID = 1L;
    static final String MARKER_FAMILY = "flink_commit";
    static final Map<String, Probe> PROBES = new ConcurrentHashMap<>();
    private final String runId;
    private final int maxEntries;
    private final long maxBytes;

    StagedMutationTestSink(String runId, int maxEntries, long maxBytes) {
        this.runId = runId;
        this.maxEntries = maxEntries;
        this.maxBytes = maxBytes;
    }

    static final class Input {
        private final String table;
        private final ByteString row;
        private final List<Mutation> mutations;

        Input(String table, ByteString row, List<Mutation> mutations) {
            this.table = table;
            this.row = row;
            this.mutations = List.copyOf(mutations);
        }

        String table() {
            return table;
        }

        ByteString row() {
            return row;
        }

        List<Mutation> mutations() {
            return mutations;
        }
    }

    static final class Probe {
        final List<CheckAndMutateRowRequest> sent = new ArrayList<>();
        final Map<String, Map<String, ByteString>> cells = new HashMap<>();
        int applied;
        int deduplicated;
        boolean loseNextAnswer;

        synchronized boolean apply(CheckAndMutateRowRequest request) throws IOException {
            sent.add(request);
            String row = request.getTableName() + "/" + request.getRowKey().toStringUtf8();
            Map<String, ByteString> stored = cells.computeIfAbsent(row, ignored -> new HashMap<>());
            RowFilter.Chain predicate = request.getPredicateFilter().getChain();
            String family = predicate.getFilters(0).getFamilyNameRegexFilter();
            String qualifier =
                    predicate.getFilters(1).getColumnQualifierRegexFilter().toStringUtf8();
            boolean matches =
                    stored.keySet().stream()
                            .anyMatch(
                                    key -> {
                                        String[] column = key.split(":", 2);
                                        return column[0].matches(family)
                                                && column[1].matches(qualifier);
                                    });
            if (matches) {
                deduplicated++;
                return true;
            }
            Map<String, ByteString> updated = new HashMap<>(stored);
            for (Mutation mutation : request.getFalseMutationsList()) {
                switch (mutation.getMutationCase()) {
                    case SET_CELL:
                        Mutation.SetCell set = mutation.getSetCell();
                        updated.put(
                                set.getFamilyName() + ":" + set.getColumnQualifier().toStringUtf8(),
                                set.getValue());
                        break;
                    case ADD_TO_CELL:
                        Mutation.AddToCell add = mutation.getAddToCell();
                        String column =
                                add.getFamilyName()
                                        + ":"
                                        + add.getColumnQualifier().getRawValue().toStringUtf8();
                        long prior =
                                Long.parseLong(
                                        updated.getOrDefault(column, ByteString.copyFromUtf8("0"))
                                                .toStringUtf8());
                        updated.put(
                                column,
                                ByteString.copyFromUtf8(
                                        Long.toString(prior + add.getInput().getIntValue())));
                        break;
                    default:
                        throw new IOException(
                                "Mutation outside the fake's exercised subset: "
                                        + mutation.getMutationCase());
                }
            }
            cells.put(row, updated);
            applied++;
            if (loseNextAnswer) {
                loseNextAnswer = false;
                throw new IOException("Applied row; response lost");
            }
            return false;
        }

        long sum(String table, String row) {
            return Long.parseLong(cells.get(table + "/" + row).get("agg:count").toStringUtf8());
        }
    }

    @Override
    public Writer createWriter(WriterInitContext context) {
        return new Writer(maxEntries, maxBytes);
    }

    @Override
    public Committer<CheckAndMutateRowRequest> createCommitter(CommitterInitContext context) {
        return new Committer<>() {
            @Override
            public void commit(Collection<CommitRequest<CheckAndMutateRowRequest>> requests)
                    throws IOException {
                for (CommitRequest<CheckAndMutateRowRequest> request : requests) {
                    if (PROBES.get(runId).apply(request.getCommittable())) {
                        request.signalAlreadyCommitted();
                    }
                }
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public SimpleVersionedSerializer<CheckAndMutateRowRequest> getCommittableSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(CheckAndMutateRowRequest request) {
                return request.toByteArray();
            }

            @Override
            public CheckAndMutateRowRequest deserialize(int version, byte[] bytes)
                    throws IOException {
                if (version != 1) {
                    throw new IOException("Unsupported probe version: " + version);
                }
                return CheckAndMutateRowRequest.parseFrom(bytes);
            }
        };
    }

    static final class Writer implements CommittingSinkWriter<Input, CheckAndMutateRowRequest> {
        private final SecureRandom random = new SecureRandom();
        private final List<CheckAndMutateRowRequest> pending = new ArrayList<>();
        private final int maxEntries;
        private final long maxBytes;
        private long bytes;

        Writer(int maxEntries, long maxBytes) {
            this.maxEntries = maxEntries;
            this.maxBytes = maxBytes;
        }

        @Override
        public void write(Input input, Context context) throws IOException {
            if (input == null) {
                return;
            }
            if (input.mutations().isEmpty() || input.mutations().size() > 99_999) {
                throw new IOException(
                        "A staged record requires 1..99,999 mutations plus its marker");
            }
            for (Mutation mutation : input.mutations()) {
                String family;
                switch (mutation.getMutationCase()) {
                    case SET_CELL:
                        family = mutation.getSetCell().getFamilyName();
                        break;
                    case ADD_TO_CELL:
                        family = mutation.getAddToCell().getFamilyName();
                        break;
                    case MERGE_TO_CELL:
                        family = mutation.getMergeToCell().getFamilyName();
                        break;
                    case DELETE_FROM_COLUMN:
                        family = mutation.getDeleteFromColumn().getFamilyName();
                        break;
                    case DELETE_FROM_FAMILY:
                        family = mutation.getDeleteFromFamily().getFamilyName();
                        break;
                    default:
                        throw new IOException(
                                "A staged mutation must preserve the marker family: "
                                        + mutation.getMutationCase());
                }
                if (MARKER_FAMILY.equals(family)) {
                    throw new IOException("Reserved marker family: " + family);
                }
            }
            byte[] identity = new byte[16];
            random.nextBytes(identity);
            StringBuilder hex = new StringBuilder(32);
            for (byte value : identity) {
                hex.append(Character.forDigit((value & 0xff) >>> 4, 16));
                hex.append(Character.forDigit(value & 15, 16));
            }
            String id = hex.toString();
            Mutation marker =
                    Mutation.newBuilder()
                            .setSetCell(
                                    Mutation.SetCell.newBuilder()
                                            .setFamilyName(MARKER_FAMILY)
                                            .setColumnQualifier(ByteString.copyFromUtf8(id))
                                            .setTimestampMicros(0)
                                            .setValue(ByteString.copyFromUtf8("1")))
                            .build();
            CheckAndMutateRowRequest request =
                    CheckAndMutateRowRequest.newBuilder()
                            .setTableName(input.table())
                            .setAppProfileId("single-cluster")
                            .setRowKey(input.row())
                            .setPredicateFilter(
                                    RowFilter.newBuilder()
                                            .setChain(
                                                    RowFilter.Chain.newBuilder()
                                                            .addFilters(
                                                                    RowFilter.newBuilder()
                                                                            .setFamilyNameRegexFilter(
                                                                                    MARKER_FAMILY))
                                                            .addFilters(
                                                                    RowFilter.newBuilder()
                                                                            .setColumnQualifierRegexFilter(
                                                                                    ByteString
                                                                                            .copyFromUtf8(
                                                                                                    id)))))
                            .addAllFalseMutations(input.mutations())
                            .addFalseMutations(marker)
                            .build();
            long charge = request.getSerializedSize() + 256L;
            if (pending.size() == maxEntries || charge > maxBytes - bytes) {
                throw new IOException(
                        "Staging capacity exceeded: maxStagedEntries="
                                + maxEntries
                                + ", maxStagedBytes="
                                + maxBytes
                                + ", entries="
                                + pending.size()
                                + ", bytes="
                                + bytes);
            }
            pending.add(request);
            bytes += charge;
        }

        long stagedBytes() {
            return bytes;
        }

        int stagedEntries() {
            return pending.size();
        }

        @Override
        public void flush(boolean endOfInput) {}

        @Override
        public Collection<CheckAndMutateRowRequest> prepareCommit() {
            List<CheckAndMutateRowRequest> result = List.copyOf(pending);
            pending.clear();
            bytes = 0;
            return result;
        }

        @Override
        public void close() {}
    }
}
