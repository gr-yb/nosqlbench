/*
 * Copyright (c) 2022 nosqlbench
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

package io.nosqlbench.virtdata.library.basics.tests.long_long;

import io.nosqlbench.virtdata.library.basics.shared.from_long.to_long.HashRange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HashRangeTest {

    @Test
    public void testFixedSize() {
        HashRange hashRange = new HashRange(65);
        assertThat(hashRange.applyAsLong(32L)).isEqualTo(11L);
    }

    @Test
    public void testSingleElementRange() {
        HashRange hashRange = new HashRange(33L,33L);
        long l = hashRange.applyAsLong(93874L);
        assertThat(l).isEqualTo(33L);
    }

}
