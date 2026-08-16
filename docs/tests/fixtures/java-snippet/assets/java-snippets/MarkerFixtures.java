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

final class MarkerFixtures {

    private int wrapperBeforeSentinel;

    private static final int firstValue = 1;
    private static final int secondValue = 2;
    private static final String firstPart = "first";
    private static final String secondPart = "second";

    private static void firstPage() {
        // tag::valid[]

        int renderedRegionSentinel =
                firstValue
                        + secondValue;

        render(renderedRegionSentinel);

        // end::valid[]
    }

    private static void secondPage(boolean enabled) {
        if (enabled) {
            // tag::valid-second[]

            String secondPageSentinel =
                    firstPart
                            + secondPart;

            render(secondPageSentinel);

            // end::valid-second[]
        }
    }

    private static void mixedWhitespacePrefixes() {
        // tag::mixed-whitespace-prefixes[]
	        String mixedWhitespacePrefixSentinel =
        firstPart
	                + secondPart;
        // end::mixed-whitespace-prefixes[]
    }

    private static void render(int value) {}

    private static void render(String value) {}

    private int wrapperAfterSentinel;

    private static final class SupportTypeSentinel {}

    // end::missing-start[]

    // tag::duplicate-start[]
    // tag::duplicate-start[]
    private int duplicateStart;
    // end::duplicate-start[]

    // tag::missing-end[]
    private int missingEnd;

    // tag::duplicate-end[]
    private int duplicateEnd;
    // end::duplicate-end[]
    // end::duplicate-end[]

    // end::reversed[]
    private int reversed;
    // tag::reversed[]

    // tag::empty[]
    // end::empty[]

    // tag::trailing-start[] private int uncheckedTrailingStart;
    // end::trailing-start[]

    // tag::trailing-end[]
    private int uncheckedTrailingEnd;
    // end::trailing-end[] private int uncheckedAfterEnd;
}
