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

package io.github.flink.gcp.connector.bigtable.source.changestream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.ByteString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

/**
 * The connector-side family and qualifier patterns, answering whether one identifier is retained.
 *
 * <p>Holds no mutation and applies nothing itself: the converter asks it per family and per
 * qualified column while it walks a complete SDK mutation, and the record emitter reads {@link
 * #hasEntryFilters()} to decide whether that walk is needed at all.
 */
@Internal
public final class BigtableChangeStreamMutationFilter implements Serializable {

    private static final long serialVersionUID = 1L;

    private final List<Pattern> familyIncludeList;
    private final List<Pattern> familyExcludeList;
    private final List<Pattern> qualifierIncludeList;
    private final List<Pattern> qualifierExcludeList;
    private final boolean skipMessagesWithoutChange;

    public static BigtableChangeStreamMutationFilter none() {
        return new BigtableChangeStreamMutationFilter(
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                false);
    }

    public BigtableChangeStreamMutationFilter(
            List<Pattern> familyIncludeList,
            List<Pattern> familyExcludeList,
            List<Pattern> qualifierIncludeList,
            List<Pattern> qualifierExcludeList,
            boolean skipMessagesWithoutChange) {
        this.familyIncludeList = immutableCopy(familyIncludeList, "familyIncludeList");
        this.familyExcludeList = immutableCopy(familyExcludeList, "familyExcludeList");
        this.qualifierIncludeList = immutableCopy(qualifierIncludeList, "qualifierIncludeList");
        this.qualifierExcludeList = immutableCopy(qualifierExcludeList, "qualifierExcludeList");
        Preconditions.checkArgument(
                this.familyIncludeList.isEmpty() || this.familyExcludeList.isEmpty(),
                "familyIncludeList and familyExcludeList must not both be set");
        Preconditions.checkArgument(
                this.qualifierIncludeList.isEmpty() || this.qualifierExcludeList.isEmpty(),
                "qualifierIncludeList and qualifierExcludeList must not both be set");
        this.skipMessagesWithoutChange = skipMessagesWithoutChange;
    }

    public boolean hasEntryFilters() {
        return !familyIncludeList.isEmpty()
                || !familyExcludeList.isEmpty()
                || !qualifierIncludeList.isEmpty()
                || !qualifierExcludeList.isEmpty();
    }

    public boolean hasQualifierFilters() {
        return !qualifierIncludeList.isEmpty() || !qualifierExcludeList.isEmpty();
    }

    public boolean includesFamily(String familyName) {
        return included(familyName, familyIncludeList, familyExcludeList);
    }

    public boolean includesQualifiedColumn(String familyName, ByteString qualifier) {
        String identifier =
                familyName + ":" + Base64.getEncoder().encodeToString(qualifier.toByteArray());
        return included(identifier, qualifierIncludeList, qualifierExcludeList);
    }

    public boolean skipsMessagesWithoutChange() {
        return skipMessagesWithoutChange;
    }

    private static boolean included(
            String identifier, List<Pattern> includes, List<Pattern> excludes) {
        if (!includes.isEmpty()) {
            for (int index = 0; index < includes.size(); index++) {
                if (includes.get(index).matcher(identifier).matches()) {
                    return true;
                }
            }
            return false;
        }
        for (int index = 0; index < excludes.size(); index++) {
            if (excludes.get(index).matcher(identifier).matches()) {
                return false;
            }
        }
        return true;
    }

    private static List<Pattern> immutableCopy(List<Pattern> patterns, String name) {
        Preconditions.checkNotNull(patterns, name + " must not be null");
        Preconditions.checkArgument(!patterns.contains(null), name + " must not contain null");
        return Collections.unmodifiableList(new ArrayList<>(patterns));
    }
}
