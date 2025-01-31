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

package io.nosqlbench.engine.api.activityapi.planning;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class BucketSequencerTest {

    @Test
    public void testBasicRatios() {
        BucketSequencer<String> buckets = new BucketSequencer<>();
        int[] ints = buckets.seqIndexesByRatios(List.of("a","b","c"), List.of(0L, 2L, 3L));
        assertThat(ints).containsExactly(1,2,1,2,2);
    }

}
