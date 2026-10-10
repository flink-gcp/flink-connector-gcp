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

package io.github.flink.gcp.connector.datastore.source.serializer;

import org.apache.flink.annotation.Internal;

import com.google.cloud.Timestamp;

import javax.annotation.Nullable;

/**
 * What a query result carries about an entity beside the entity itself, and the read time of the
 * split that read it: none of it is on the client library's {@code Entity}. The {@code datastore}
 * table's lookup fills it from a lookup's answer, whose read time is the answer's own.
 */
@Internal
public final class EntityMetadata {

    private final long version;
    @Nullable private final Timestamp createTime;
    @Nullable private final Timestamp updateTime;
    @Nullable private final Timestamp readTime;

    /**
     * Creates the metadata.
     *
     * @param version the entity's version, which grows with every change to the entity
     * @param createTime when the entity was created, or {@code null} when the result has none
     * @param updateTime when the entity was last changed, or {@code null} when the result has none
     * @param readTime the time the entity was read at, or {@code null} when the answer has none
     */
    public EntityMetadata(
            long version,
            @Nullable Timestamp createTime,
            @Nullable Timestamp updateTime,
            @Nullable Timestamp readTime) {
        this.version = version;
        this.createTime = createTime;
        this.updateTime = updateTime;
        this.readTime = readTime;
    }

    /** Returns the entity's version. */
    public long getVersion() {
        return version;
    }

    /** Returns when the entity was created, or {@code null} when the result has none. */
    @Nullable
    public Timestamp getCreateTime() {
        return createTime;
    }

    /** Returns when the entity was last changed, or {@code null} when the result has none. */
    @Nullable
    public Timestamp getUpdateTime() {
        return updateTime;
    }

    /** Returns the time the entity was read at, or {@code null} when the answer has none. */
    @Nullable
    public Timestamp getReadTime() {
        return readTime;
    }
}
