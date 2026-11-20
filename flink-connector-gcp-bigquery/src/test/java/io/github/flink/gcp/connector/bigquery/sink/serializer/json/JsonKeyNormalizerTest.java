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

import com.google.cloud.bigquery.storage.v1.BigQuerySchemaUtil;
import com.google.cloud.bigquery.storage.v1.TableFieldSchema;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link JsonKeyNormalizer}, passing the locale directly rather than changing the JVM's
 * default.
 */
class JsonKeyNormalizerTest {

    private static final Locale TURKISH = new Locale("tr", "TR");

    private static TableFieldSchema field(String name, TableFieldSchema.Type type) {
        return TableFieldSchema.newBuilder().setName(name).setType(type).build();
    }

    /**
     * A repeated {@code STRUCT} holding a {@code STRUCT}, so that both levels are walked, and a
     * column for each key the tests below expect renamed.
     */
    private static JsonKeyNormalizer normalizer() {
        return JsonKeyNormalizer.of(
                TableSchema.newBuilder()
                        .addFields(field("userId", TableFieldSchema.Type.INT64))
                        .addFields(field("ID", TableFieldSchema.Type.INT64))
                        .addFields(field("I", TableFieldSchema.Type.INT64))
                        .addFields(field("Name", TableFieldSchema.Type.INT64))
                        .addFields(field("XI", TableFieldSchema.Type.INT64))
                        .addFields(field("IDS", TableFieldSchema.Type.INT64))
                        .addFields(field("Iban", TableFieldSchema.Type.STRING))
                        .addFields(field("Ip", TableFieldSchema.Type.JSON))
                        .addFields(field("\u00CDnfo", TableFieldSchema.Type.INT64))
                        .addFields(field("i\u03B1\u03C2", TableFieldSchema.Type.INT64))
                        .addFields(field("\u0391\u03A3\u0E01\u0392", TableFieldSchema.Type.INT64))
                        .addFields(
                                field("Items", TableFieldSchema.Type.STRUCT).toBuilder()
                                        .setMode(TableFieldSchema.Mode.REPEATED)
                                        .addFields(
                                                field("Info", TableFieldSchema.Type.STRUCT)
                                                        .toBuilder()
                                                        .addFields(
                                                                field(
                                                                        "Id",
                                                                        TableFieldSchema.Type
                                                                                .INT64))
                                                        .build())
                                        .build())
                        .build());
    }

    /**
     * Normalizes the document twice with one normalizer, so that the first pass decides each key
     * and the second meets the cached decisions, and requires the two to agree.
     */
    private static JSONObject normalized(String json, Locale locale) throws IOException {
        JsonKeyNormalizer normalizer = normalizer();
        JSONObject decided = new JSONObject(json);
        normalizer.normalize(decided, locale);
        int cachedKeys = normalizer.cachedKeys();
        assertThat(cachedKeys).as("keys cached after deciding %s", json).isPositive();
        JSONObject cached = new JSONObject(json);
        normalizer.normalize(cached, locale);
        assertThat(normalizer.cachedKeys())
                .as("keys cached after meeting them again")
                .isEqualTo(cachedKeys);
        assertThat(cached.similar(decided)).as("cached %s, decided %s", cached, decided).isTrue();
        return cached;
    }

    /**
     * The decisions are cached per locale: one made under English, where nothing is renamed, must
     * not be reused under Turkish, nor one made under Turkish under English.
     */
    @Test
    void decidesAgainWhenTheLocaleChanges() throws Exception {
        JsonKeyNormalizer normalizer = normalizer();
        for (Locale locale : new Locale[] {Locale.US, TURKISH, Locale.US}) {
            JSONObject document = new JSONObject("{\"userId\":1}");
            normalizer.normalize(document, locale);
            assertThat(document.keySet())
                    .as("under %s", locale)
                    .containsExactly(locale.equals(TURKISH) ? "userid" : "userId");
        }
    }

    /** A key met once the cache is full is still decided, only not cached. */
    @Test
    void decidesKeysMetOnceTheCacheIsFull() throws Exception {
        JsonKeyNormalizer normalizer = normalizer();
        JSONObject unknownKeys = new JSONObject();
        for (int i = 0; i < 4096; i++) {
            unknownKeys.put("unknown" + i, i);
        }
        normalizer.normalize(unknownKeys, TURKISH);
        // Twice the 12 top-level fields plus 1,024, far fewer than the keys met.
        assertThat(normalizer.cachedKeys()).isEqualTo(2 * 12 + 1024);
        JSONObject document = new JSONObject("{\"userId\":1}");

        normalizer.normalize(document, TURKISH);

        assertThat(document.keySet()).containsExactly("userid");
    }

    /**
     * A key longer than twice the longest field name is decided as met, not cached, so that long
     * keys cannot hold memory; a key that names a column is cached.
     */
    @Test
    void leavesLongKeysOutOfTheCache() throws Exception {
        JsonKeyNormalizer normalizer = normalizer();
        JSONObject document = new JSONObject();
        document.put("I".repeat(1000), 1);

        normalizer.normalize(document, TURKISH);

        assertThat(normalizer.cachedKeys()).isZero();
        assertThat(document.keySet()).containsExactly("I".repeat(1000));
        normalizer.normalize(new JSONObject("{\"userId\":1}"), TURKISH);
        assertThat(normalizer.cachedKeys()).isOne();
    }

    /**
     * Two keys for one column are respelled in turn, each against the keys the one before left, so
     * neither takes the other's spelling and both values stay.
     */
    @Test
    void respellsTwoKeysForOneColumnWithoutLosingEither() throws Exception {
        JSONObject document = normalized("{\"ID\":1,\"Id\":2}", TURKISH);

        assertThat(document.keySet()).containsExactlyInAnyOrder("id", "iD");
        assertThat(List.of(document.getInt("id"), document.getInt("iD")))
                .containsExactlyInAnyOrder(1, 2);
    }

    @Test
    void leavesEveryKeyAsItIsUnderAnEnglishLocale() throws Exception {
        JSONObject document =
                normalized("{\"userId\":1,\"Items\":[{\"Info\":{\"Id\":2}}]}", Locale.US);

        assertThat(document.toString())
                .isEqualTo(
                        new JSONObject("{\"userId\":1,\"Items\":[{\"Info\":{\"Id\":2}}]}")
                                .toString());
    }

    @Test
    void renamesKeysHoldingAnUpperCaseIAtEveryStructLevelUnderTurkish() throws Exception {
        JSONObject document =
                normalized("{\"userId\":1,\"Items\":[{\"Info\":{\"Id\":2}}]}", TURKISH);

        assertThat(document.keySet()).containsExactlyInAnyOrder("userid", "items");
        JSONObject info = document.getJSONArray("items").getJSONObject(0).getJSONObject("info");
        assertThat(info.keySet()).containsExactly("id");
        assertThat(info.getInt("id")).isEqualTo(2);
    }

    /** A key whose lower-casings agree is left as it is, upper-case letters included. */
    @Test
    void leavesAKeyWithoutAnUpperCaseIAsItIsUnderTurkish() throws Exception {
        assertThat(normalized("{\"Name\":1}", TURKISH).keySet()).containsExactly("Name");
    }

    @Test
    void renamesUnderAzerbaijaniAsUnderTurkish() throws Exception {
        assertThat(normalized("{\"userId\":1}", new Locale("az", "AZ")).keySet())
                .containsExactly("userid");
    }

    /**
     * Lithuanian keeps the dot of an accented capital I, so {@code Í} lower-cases to three chars.
     */
    @Test
    void renamesAnAccentedCapitalIUnderLithuanian() throws Exception {
        Locale lithuanian = new Locale("lt", "LT");
        String key = "Ínfo";
        // The premise: the two lower-casings of this key differ.
        assertThat(key.toLowerCase(lithuanian)).isNotEqualTo(key.toLowerCase(Locale.ROOT));

        assertThat(normalized("{\"" + key + "\":1}", lithuanian).keySet()).containsExactly("ínfo");
    }

    /**
     * Greek final sigma depends on the locale's word boundaries, and the JDK's Thai ones differ, so
     * the check is made per key rather than against a list of languages.
     */
    @Test
    void renamesAKeyWhoseFinalSigmaDependsOnThaiWordBoundaries() throws Exception {
        Locale thai = new Locale("th", "TH");
        String key = "ΑΣกΒ";
        // The premise: the two lower-casings of this key differ.
        assertThat(key.toLowerCase(thai)).isNotEqualTo(key.toLowerCase(Locale.ROOT));

        assertThat(normalized("{\"" + key + "\":1}", thai).keySet())
                .containsExactly(key.toLowerCase(Locale.ROOT));
    }

    /**
     * A renamed key whose lower-case form the document already holds takes another spelling the
     * locale lower-cases the same way, so both keys reach the library with their values intact.
     */
    @Test
    void respellsARenamedKeyThatWouldMeetAKeyAlreadyThere() throws Exception {
        JSONObject document = normalized("{\"ID\":1,\"id\":2}", TURKISH);

        assertThat(document.keySet()).containsExactlyInAnyOrder("id", "iD");
        assertThat(document.getInt("iD")).isEqualTo(1);
        assertThat(document.getInt("id")).isEqualTo(2);
        assertThat("iD".toLowerCase(TURKISH)).isEqualTo("id");
    }

    @Test
    void respellsARepeatedColumnsKeyWithoutJoiningTheArrays() throws Exception {
        JSONObject document =
                normalized(
                        "{\"Items\":[{\"Info\":{\"Id\":1}}],\"items\":[{\"info\":{\"id\":2}}]}",
                        TURKISH);

        assertThat(document.keySet()).containsExactlyInAnyOrder("items", "iTems");
        assertThat(document.getJSONArray("items").length()).isEqualTo(1);
        assertThat(document.getJSONArray("iTems").length()).isEqualTo(1);
        // The respelled key's elements are still walked.
        assertThat(document.getJSONArray("iTems").getJSONObject(0).getJSONObject("info").keySet())
                .containsExactly("id");
    }

    /** The first letter that upper-cases safely is the one upper-cased, at any position. */
    @Test
    void upperCasesTheFirstLetterTheLocaleLowerCasesBack() throws Exception {
        assertThat(normalized("{\"XI\":1,\"xi\":2}", TURKISH).keySet())
                .containsExactlyInAnyOrder("xi", "Xi");
    }

    /**
     * A spelling another key already holds is not taken, and with none left the record is refused
     * rather than left with a key that matches nothing, which unknown-field handling would drop.
     */
    @Test
    void refusesARecordWhenEverySpellingIsTaken() {
        assertThatThrownBy(() -> normalized("{\"ID\":1,\"id\":2,\"iD\":3}", TURKISH))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("the column id")
                .hasMessageContaining("ID");
        assertThatThrownBy(() -> normalized("{\"I\":1,\"i\":2}", TURKISH))
                .isInstanceOf(IOException.class);
    }

    /**
     * A key that names a column only under the default locale is renamed too, so that it reaches no
     * column, as under {@link Locale#ROOT}: under Turkish {@code İD} would reach the column {@code
     * id}.
     */
    @Test
    void respellsAKeyThatNamesAColumnOnlyUnderTheDefaultLocale() throws Exception {
        JSONObject document = new JSONObject("{\"\u0130D\":1}");
        JsonKeyNormalizer.of(
                        TableSchema.newBuilder()
                                .addFields(field("id", TableFieldSchema.Type.INT64))
                                .build())
                .normalize(document, TURKISH);

        assertThat(document.keySet()).containsExactly("i\u0307d");
    }

    /**
     * A key whose default-locale form reaches a column through the placeholder the client library
     * names a column that is not protobuf-compatible after is renamed as well, so that it reaches
     * no field, as under {@link Locale#ROOT}.
     */
    @Test
    void respellsAKeyThatReachesAColumnThroughItsPlaceholderUnderTheDefaultLocale()
            throws Exception {
        assertThat(BigQuerySchemaUtil.generatePlaceholderFieldName("\u5BA2")).isEqualTo("col_5a6i");
        JSONObject document = new JSONObject("{\"col_5a6\u0130\":1}");
        JsonKeyNormalizer.of(
                        TableSchema.newBuilder()
                                .addFields(field("\u5BA2", TableFieldSchema.Type.INT64))
                                .build())
                .normalize(document, TURKISH);

        assertThat(document.keySet()).containsExactly("col_5a6i\u0307");
    }

    /**
     * A key whose default-locale form is a column name that is not protobuf-compatible reaches that
     * column through its placeholder, so it is renamed too: under Turkish {@code ID} would reach
     * the column {@code ıd}, which under {@link Locale#ROOT} it does not.
     */
    @Test
    void respellsAKeyWhoseDefaultLocaleFormIsANameReachedThroughAPlaceholder() throws Exception {
        JSONObject document = new JSONObject("{\"ID\":1}");
        JsonKeyNormalizer.of(
                        TableSchema.newBuilder()
                                .addFields(field("\u0131d", TableFieldSchema.Type.INT64))
                                .build())
                .normalize(document, TURKISH);

        assertThat(document.keySet()).containsExactly("id");
    }

    /**
     * A {@code STRUCT} column whose name is not protobuf-compatible is walked whether a key spells
     * its name or its placeholder.
     */
    @Test
    void walksAStructColumnReachedThroughItsPlaceholder() throws Exception {
        JSONObject document = new JSONObject("{\"col_5a6i\":{\"ID\":1},\"\u5BA2\":{\"ID\":2}}");
        JsonKeyNormalizer.of(
                        TableSchema.newBuilder()
                                .addFields(
                                        field("\u5BA2", TableFieldSchema.Type.STRUCT).toBuilder()
                                                .addFields(
                                                        field("id", TableFieldSchema.Type.INT64)))
                                .build())
                .normalize(document, TURKISH);

        assertThat(document.getJSONObject("col_5a6i").keySet()).containsExactly("id");
        assertThat(document.getJSONObject("\u5BA2").keySet()).containsExactly("id");
    }

    /**
     * A key reaching no field under either lower-casing is unknown to the library under the default
     * locale as under {@link Locale#ROOT}, so it is left as it is, however long, and with no
     * spelling searched for it.
     */
    @Test
    void leavesKeysThatNameNoColumnUnderEitherLowerCasingAsTheyAre() throws Exception {
        String longKey = "I" + "0".repeat(100_000);
        JSONObject document = new JSONObject();
        document.put("IX", 1);
        document.put("ix", 2);
        document.put("I", 3);
        document.put("i", 4);
        document.put(longKey, 5);
        document.put(longKey.toLowerCase(Locale.ROOT), 6);
        JsonKeyNormalizer.of(
                        TableSchema.newBuilder()
                                .addFields(field("ok", TableFieldSchema.Type.INT64))
                                .build())
                .normalize(document, TURKISH);

        assertThat(document.keySet())
                .containsExactlyInAnyOrder(
                        "IX", "ix", "I", "i", longKey, longKey.toLowerCase(Locale.ROOT));
    }

    /**
     * Whether a spelling lower-cases back is decided on the whole spelling: a final capital sigma
     * lower-cases to the final form only in context, which a letter-by-letter check would miss.
     */
    @Test
    void findsASpellingWhoseLowerCaseDependsOnContext() throws Exception {
        JSONObject document =
                normalized(
                        "{\"I\u03B1\u03C2\":1,\"i\u03B1\u03C2\":2,\"i\u0391\u03C2\":3}", TURKISH);

        assertThat(document.keySet())
                .containsExactlyInAnyOrder("i\u03B1\u03C2", "i\u0391\u03C2", "i\u03B1\u03A3");
        assertThat("i\u03B1\u03A3".toLowerCase(TURKISH)).isEqualTo("i\u03B1\u03C2");
    }

    /**
     * A letter whose upper case never lower-cases back, such as an i under Turkish, is no
     * candidate, so a long run of them does not use up the tries before a free spelling is reached.
     */
    @Test
    void skipsLettersThatNeverLowerCaseBack() throws Exception {
        String prefix = "i".repeat(89);
        String column = prefix + "ab";
        JSONObject document = new JSONObject();
        document.put(column, 1);
        document.put(prefix + "Ab", 2);
        document.put(prefix + "aB", 3);
        document.put("I" + "i".repeat(88) + "ab", 4);
        JsonKeyNormalizer.of(
                        TableSchema.newBuilder()
                                .addFields(field(column, TableFieldSchema.Type.INT64))
                                .build())
                .normalize(document, TURKISH);

        assertThat(document.keySet())
                .containsExactlyInAnyOrder(column, prefix + "Ab", prefix + "aB", prefix + "AB");
        assertThat(document.getInt(prefix + "AB")).isEqualTo(4);
    }

    /** With the single-letter spellings taken, a spelling with two letters upper-cased is used. */
    @Test
    void upperCasesMoreLettersWhenTheSingleLetterSpellingsAreTaken() throws Exception {
        JSONObject document = normalized("{\"ids\":1,\"iDs\":2,\"idS\":3,\"IDS\":4}", TURKISH);

        assertThat(document.keySet()).containsExactlyInAnyOrder("ids", "iDs", "idS", "iDS");
        assertThat(document.getInt("iDS")).isEqualTo(4);
    }

    /** Values are never changed, whether text, a number or a JSON column's own document. */
    @Test
    void neverChangesAValue() throws Exception {
        JSONObject document =
                normalized("{\"Iban\":\"TR12AB\",\"Ip\":\"{\\\"Id\\\":1}\"}", TURKISH);

        assertThat(document.getString("iban")).isEqualTo("TR12AB");
        assertThat(document.getString("ip")).isEqualTo("{\"Id\":1}");
    }
}
