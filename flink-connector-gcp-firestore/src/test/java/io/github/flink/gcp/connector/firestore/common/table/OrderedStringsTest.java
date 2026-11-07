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

package io.github.flink.gcp.connector.firestore.common.table;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests which string literals every value compares with alike in UTF-8 and UTF-16 order. */
class OrderedStringsTest {

    @Test
    void aLiteralBelowU0000D800IsOrderedAlikeAndOneAtOrAboveItIsNot() {
        assertThat(OrderedStrings.isOrderedAlike("")).isTrue();
        assertThat(OrderedStrings.isOrderedAlike("abcé퟿")).isTrue();
        assertThat(OrderedStrings.isOrderedAlike("a")).isFalse();
        assertThat(OrderedStrings.isOrderedAlike("�")).isFalse();
        assertThat(OrderedStrings.isOrderedAlike("😀")).isFalse();
    }

    @Test
    void theTwoOrdersDisagreeOnlyOutsideTheLiteralsThatAreOrderedAlike() {
        // Firing control: the orders do disagree on these two.
        assertThat(Integer.signum("😀".compareTo("�")))
                .isNotEqualTo(Integer.signum(utf8("😀", "�")));

        // Random strings over code points that straddle every boundary, each compared with a
        // random literal ordered alike: UTF-16 and UTF-8 order the pair the same way.
        int[] points = {'a', 0x7f, 0xe9, 0xD7FF, 0xE000, 0xFFFD, 0x1F600, 0x10FFFF};
        Random random = new Random(7);
        for (int i = 0; i < 20_000; i++) {
            String value = random(random, points, false);
            String literal = random(random, points, true);
            assertThat(OrderedStrings.isOrderedAlike(literal)).isTrue();
            assertThat(Integer.signum(value.compareTo(literal)))
                    .as("%s vs %s", value, literal)
                    .isEqualTo(Integer.signum(utf8(value, literal)));
        }
    }

    private static String random(Random random, int[] points, boolean belowSurrogates) {
        StringBuilder out = new StringBuilder();
        int length = random.nextInt(4);
        for (int i = 0; i < length; i++) {
            int point = points[random.nextInt(points.length)];
            if (belowSurrogates && point >= 0xD800) {
                point = 0xD7FF;
            }
            out.appendCodePoint(point);
        }
        return out.toString();
    }

    private static int utf8(String a, String b) {
        return Arrays.compareUnsigned(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
