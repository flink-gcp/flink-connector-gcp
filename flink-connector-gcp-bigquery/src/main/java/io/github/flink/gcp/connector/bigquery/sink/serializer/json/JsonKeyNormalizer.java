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

package io.github.flink.gcp.connector.bigquery.sink.serializer.json;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import io.github.flink.gcp.connector.bigquery.sink.serializer.RowDescriptors;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Renames the keys of a parsed JSON document that the client library would lower-case differently
 * under the JVM's default locale than under {@link Locale#ROOT}.
 *
 * <p>{@code JsonToProtoMessage} lower-cases each key with the default locale and looks the result
 * up in the row descriptor, whose field names are the column names lower-cased as under {@link
 * Locale#ROOT} ({@code RowDescriptors.derive}), by that form or, where it is not
 * protobuf-compatible, by its {@code col_} placeholder ({@code RowDescriptors.fieldName}). The two
 * lower-casings differ under a locale with language-sensitive case mappings, which Unicode's {@code
 * SpecialCasing.txt} defines for Lithuanian, Turkish and Azerbaijani; the JDK's word boundaries for
 * Greek final sigma add Thai. Under Turkish the key {@code userId} becomes {@code userıd}, which
 * matches no field: the record fails as carrying an unknown field, or loses that value when unknown
 * fields are ignored.
 *
 * <p>Each key that reaches a field under either lower-casing, and whose two lower-casings differ,
 * is therefore renamed to a spelling the default locale lower-cases to the key's {@link
 * Locale#ROOT} form, at the top level and inside every {@code STRUCT} column the schema declares,
 * repeated ones included, so that the library finds the field the key reaches under {@link
 * Locale#ROOT} and no other. Under Turkish the key {@code İD} reaches no field under {@link
 * Locale#ROOT} but would reach the column {@code id}, and {@code col_5a6İ} would reach the column
 * {@code 客} through its placeholder {@code col_5a6i}; renamed, neither reaches one. A key that
 * reaches no field under either lower-casing is unknown to the library under the default locale as
 * under {@link Locale#ROOT}, so it is left as it is, and so is a key whose lower-casings agree,
 * every key under most locales. Values are never changed, so the text of a {@code JSON} column
 * keeps its own keys. The spelling is the {@link Locale#ROOT} form itself unless the document
 * already holds that key, in which case letters of it are upper-cased instead, as few as will give
 * a free spelling, so that {@code ID} beside {@code id} becomes {@code iD}. No value is merged or
 * dropped: the library meets every key and treats two keys naming one column as under a locale that
 * lower-cases them alike, skipping a null, validating each value, appending both to a repeated
 * column and setting a singular one twice. Which value a singular column keeps, and the order a
 * repeated one takes the elements in, follow the order org.json holds the keys in, which no locale
 * defines; renaming can change that order. A document with more keys for one column than spellings
 * can be found for fails the record rather than leaving a key that reaches a column it does not
 * name or a value unmatched.
 *
 * <p>What a key needs, a respelling and a normalizer for its value, depends only on the key, the
 * locale and the schema, so each level caches it per key and starts afresh when the locale changes.
 * A record then costs a lookup per key rather than two lower-casings. A key met for the first time
 * is decided as it is met, and so is every key the cache leaves out: those beyond twice the level's
 * field count plus 1,024, and those longer than twice its longest field name. The count is checked
 * before a key is added rather than with it, so callers that miss at once can each add one more.
 */
@Internal
final class JsonKeyNormalizer {

    /** The most spellings tried for one key before the record is refused. */
    private static final int MAX_SPELLINGS = 4096;

    /**
     * The keys each level caches beyond two per field, room for keys that name no column without
     * letting a stream of ever-new keys grow the cache without bound.
     */
    private static final int SPARE_CACHED_KEYS = 1024;

    /** The decision for a key that needs nothing: no respelling and no value to walk. */
    private static final Decision NOTHING = new Decision(null, null);

    /**
     * The descriptor's field names at this level: each column's name lower-cased under {@link
     * Locale#ROOT}, or its placeholder ({@code RowDescriptors.fieldName}).
     */
    private final Set<String> fields;

    /** The normalizer of each {@code STRUCT} column, keyed by its field name. */
    private final Map<String, JsonKeyNormalizer> structFields;

    /**
     * The most keys whose decisions this level caches, give or take one per caller adding a key
     * concurrently.
     */
    private final int maxCachedKeys;

    /**
     * The longest key this level caches: twice its longest field name. A key reaching a field is
     * about as long as the field's name, so a longer one almost always names no column, and caching
     * such keys would let a stream of long keys hold memory. It is still decided, only not cached.
     */
    private final int maxCachedKeyLength;

    /** The decisions made under the locale last asked for, replaced when another is asked for. */
    @Nullable private volatile Decisions decisions;

    private JsonKeyNormalizer(List<TableFieldSchema> columns) {
        Set<String> names = new HashSet<>();
        Map<String, JsonKeyNormalizer> structs = new HashMap<>();
        for (TableFieldSchema column : columns) {
            String name = RowDescriptors.fieldName(column.getName().toLowerCase(Locale.ROOT));
            names.add(name);
            if (column.getType() == TableFieldSchema.Type.STRUCT) {
                structs.put(name, new JsonKeyNormalizer(column.getFieldsList()));
            }
        }
        this.fields = names;
        this.structFields = structs;
        this.maxCachedKeys = 2 * names.size() + SPARE_CACHED_KEYS;
        this.maxCachedKeyLength = 2 * names.stream().mapToInt(String::length).max().orElse(0);
    }

    /**
     * Creates the normalizer of documents written against the given schema.
     *
     * @param tableSchema the schema the documents are converted against
     * @return the normalizer
     */
    static JsonKeyNormalizer of(TableSchema tableSchema) {
        return new JsonKeyNormalizer(tableSchema.getFieldsList());
    }

    /**
     * Renames the keys of the document, in place, that reach a field under either lower-casing and
     * lower-case differently under the given locale than under {@link Locale#ROOT}.
     *
     * @param json the parsed document
     * @param locale the locale the client library lower-cases keys with, the default one
     * @throws IOException if a key needs renaming and no free spelling is found for it
     */
    void normalize(JSONObject json, Locale locale) throws IOException {
        Map<String, Decision> decided = decisions(locale);
        // Renamed once the walk is done, since renaming a key changes the set being walked.
        Map<String, String> respellings = null;
        for (String key : json.keySet()) {
            Decision decision = decided.get(key);
            if (decision == null) {
                decision = decide(key, locale);
                if (decided.size() < maxCachedKeys && key.length() <= maxCachedKeyLength) {
                    decided.putIfAbsent(key, decision);
                }
            }
            if (decision.struct != null) {
                decision.struct.normalizeValue(json.get(key), locale);
            }
            if (decision.rootKey != null) {
                if (respellings == null) {
                    respellings = new LinkedHashMap<>();
                }
                respellings.put(key, decision.rootKey);
            }
        }
        if (respellings == null) {
            return;
        }
        for (Map.Entry<String, String> respelling : respellings.entrySet()) {
            String key = respelling.getKey();
            String rootKey = respelling.getValue();
            String spelling = freeSpelling(json, rootKey, locale);
            if (spelling == null) {
                throw new IOException(
                        "Record has more keys spelling the column "
                                + rootKey
                                + " than can be told apart under the default locale "
                                + locale.toLanguageTag()
                                + ", among them "
                                + key);
            }
            json.put(spelling, json.remove(key));
        }
    }

    /** Returns how many keys this level holds decisions for, under the locale last asked for. */
    @VisibleForTesting
    int cachedKeys() {
        Decisions current = decisions;
        return current == null ? 0 : current.byKey.size();
    }

    private Map<String, Decision> decisions(Locale locale) {
        Decisions current = decisions;
        if (current == null || !current.locale.equals(locale)) {
            current = new Decisions(locale);
            decisions = current;
        }
        return current.byKey;
    }

    /** Decides what a key needs under the locale, as {@link #normalize} applies it. */
    private Decision decide(String key, Locale locale) {
        // The library resolves a key by the field name of its lower-cased form, a placeholder
        // where that form is not protobuf-compatible, and the key keeps its Locale.ROOT form's.
        String rootKey = key.toLowerCase(Locale.ROOT);
        String rootField = RowDescriptors.fieldName(rootKey);
        JsonKeyNormalizer struct = structFields.get(rootField);
        String localeKey = key.toLowerCase(locale);
        // A key that reaches no field under either lower-casing is unknown either way; leaving
        // it also bounds the search for a spelling by the length of a field name, not of a
        // record's key.
        boolean respell =
                !rootKey.equals(localeKey)
                        && (fields.contains(rootField)
                                || fields.contains(RowDescriptors.fieldName(localeKey)));
        if (struct == null && !respell) {
            return NOTHING;
        }
        return new Decision(struct, respell ? rootKey : null);
    }

    /**
     * Returns a key the document does not hold that lower-cases under the locale to the given lower
     * case form: the form itself, or the form with as few letters upper-cased as give a free
     * spelling, or {@code null} if none is found within {@code MAX_SPELLINGS} tries.
     */
    @Nullable
    private static String freeSpelling(JSONObject json, String lowerCase, Locale locale) {
        if (!json.has(lowerCase)) {
            return lowerCase;
        }
        // The letters whose upper case lower-cases back under the locale, each tested in its place
        // in the whole form, since some mappings, such as Greek final sigma, depend on the letters
        // around them. Those contexts turn on whether a neighbour is cased, or is a combining mark,
        // which upper-casing another letter does not change, so a spelling made of such letters
        // lower-cases back too; it is checked whole all the same. Testing in place also keeps
        // letters that can never lower-case back, such as a Turkish i, from using up the tries.
        List<Integer> letters = new ArrayList<>();
        for (int i = 0; i < lowerCase.length(); i++) {
            char upper = Character.toUpperCase(lowerCase.charAt(i));
            if (upper == lowerCase.charAt(i)) {
                continue;
            }
            char[] spelling = lowerCase.toCharArray();
            spelling[i] = upper;
            if (new String(spelling).toLowerCase(locale).equals(lowerCase)) {
                letters.add(i);
            }
        }
        int tries = 0;
        for (int count = 1; count <= letters.size(); count++) {
            int[] chosen = new int[count];
            for (int i = 0; i < count; i++) {
                chosen[i] = i;
            }
            while (true) {
                char[] spelling = lowerCase.toCharArray();
                for (int index : chosen) {
                    int position = letters.get(index);
                    spelling[position] = Character.toUpperCase(spelling[position]);
                }
                String candidate = new String(spelling);
                if (!json.has(candidate) && candidate.toLowerCase(locale).equals(lowerCase)) {
                    return candidate;
                }
                if (++tries >= MAX_SPELLINGS || !nextCombination(chosen, letters.size())) {
                    break;
                }
            }
            if (tries >= MAX_SPELLINGS) {
                return null;
            }
        }
        return null;
    }

    /** Advances to the next combination of indices in lexicographic order, if there is one. */
    private static boolean nextCombination(int[] chosen, int size) {
        for (int i = chosen.length - 1; i >= 0; i--) {
            if (chosen[i] < size - chosen.length + i) {
                chosen[i]++;
                for (int j = i + 1; j < chosen.length; j++) {
                    chosen[j] = chosen[j - 1] + 1;
                }
                return true;
            }
        }
        return false;
    }

    private void normalizeValue(Object value, Locale locale) throws IOException {
        if (value instanceof JSONObject) {
            normalize((JSONObject) value, locale);
        } else if (value instanceof JSONArray) {
            for (Object element : (JSONArray) value) {
                if (element instanceof JSONObject) {
                    normalize((JSONObject) element, locale);
                }
            }
        }
    }

    /** What {@link #normalize} does with one key. */
    private static final class Decision {
        /** The normalizer of the key's value, if the key reaches a {@code STRUCT} column. */
        @Nullable private final JsonKeyNormalizer struct;

        /** The {@link Locale#ROOT} form to respell the key to, if it needs respelling. */
        @Nullable private final String rootKey;

        Decision(@Nullable JsonKeyNormalizer struct, @Nullable String rootKey) {
            this.struct = struct;
            this.rootKey = rootKey;
        }
    }

    /** The decisions made under one locale, keyed by the key as the document spells it. */
    private static final class Decisions {
        private final Locale locale;
        private final Map<String, Decision> byKey = new ConcurrentHashMap<>();

        Decisions(Locale locale) {
            this.locale = locale;
        }
    }
}
