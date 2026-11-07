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

import org.apache.flink.annotation.Internal;

/**
 * Decides whether a string range compares a literal alike in the two orders it may meet.
 *
 * <p>Both modes order strings by their UTF-8 bytes (measured on the service, 2026-11-07), and so
 * does Flink for strings it holds as bytes; but Flink compares two strings it still holds as Java
 * strings with {@code String.compareTo}, by UTF-16 code units ({@code BinaryStringData.compareTo}),
 * and the converter's values and the planner's literals are both such strings. The orders differ
 * only where a supplementary character, a surrogate pair in UTF-16, meets a character from U+E000
 * to U+FFFF at the first position two strings differ. A literal of characters below U+D800 alone
 * never holds either at that position, so any value compares with it alike in both orders: a range
 * on it neither drops a value Flink keeps under one order nor admits one it would not under the
 * other.
 */
@Internal
public final class OrderedStrings {

    private OrderedStrings() {}

    /**
     * Returns whether a comparison by the operator may be pushed with the literal: any comparison
     * but a range needs no order, and a range needs a literal {@link #isOrderedAlike(String)}.
     *
     * @param operator the comparison's operator
     * @param literal the literal
     * @return whether the comparison may be pushed
     */
    public static boolean admits(PredicateMatcher.Operator operator, String literal) {
        return !operator.isRange() || isOrderedAlike(literal);
    }

    /**
     * Returns whether every value compares with the literal alike by UTF-8 bytes and by UTF-16 code
     * units.
     *
     * @param literal the literal
     * @return whether every character of the literal is below U+D800
     */
    public static boolean isOrderedAlike(String literal) {
        for (int i = 0; i < literal.length(); i++) {
            if (literal.charAt(i) >= '\uD800') {
                return false;
            }
        }
        return true;
    }
}
