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

package io.github.flink.gcp.connector.spanner.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/** One child-partitions record discovered while a reader is consuming a parent partition. */
@Internal
public final class ChildPartitionsEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final String parentSplitId;
    private final Instant startTimestamp;
    private final List<ChildPartition> children;

    public ChildPartitionsEvent(
            String parentSplitId, Instant startTimestamp, List<ChildPartition> children) {
        this.parentSplitId =
                Preconditions.checkNotNull(parentSplitId, "parentSplitId must not be null");
        this.startTimestamp =
                Preconditions.checkNotNull(startTimestamp, "startTimestamp must not be null");
        Preconditions.checkNotNull(children, "children must not be null");
        Preconditions.checkArgument(!children.isEmpty(), "children must not be empty");
        this.children = Collections.unmodifiableList(new ArrayList<>(children));
    }

    public String getParentSplitId() {
        return parentSplitId;
    }

    public Instant getStartTimestamp() {
        return startTimestamp;
    }

    public List<ChildPartition> getChildren() {
        return children;
    }

    /** One child token and the normalized coordinator identities of every parent. */
    @Internal
    public static final class ChildPartition implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String token;
        private final List<String> parentPartitionIds;

        public ChildPartition(String token, List<String> parentPartitionIds) {
            this.token = Preconditions.checkNotNull(token, "token must not be null");
            Preconditions.checkArgument(!token.isEmpty(), "token must not be empty");
            Preconditions.checkNotNull(parentPartitionIds, "parentPartitionIds must not be null");
            Preconditions.checkArgument(
                    !parentPartitionIds.isEmpty(), "parentPartitionIds must not be empty");
            Preconditions.checkArgument(
                    !parentPartitionIds.contains(null), "parentPartitionIds must not contain null");
            Preconditions.checkArgument(
                    !parentPartitionIds.contains(""),
                    "parentPartitionIds must not contain empty ids");
            List<String> normalizedParents =
                    new ArrayList<>(new LinkedHashSet<>(parentPartitionIds));
            Collections.sort(normalizedParents);
            this.parentPartitionIds = Collections.unmodifiableList(normalizedParents);
        }

        public String getToken() {
            return token;
        }

        public List<String> getParentPartitionIds() {
            return parentPartitionIds;
        }
    }
}
