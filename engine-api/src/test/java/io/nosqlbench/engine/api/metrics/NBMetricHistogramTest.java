/*
 * Copyright (c) 2022-2023 nosqlbench
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

package io.nosqlbench.engine.api.metrics;

import io.nosqlbench.api.engine.metrics.ConvenientSnapshot;
import io.nosqlbench.api.engine.metrics.DeltaHdrHistogramReservoir;
import io.nosqlbench.api.engine.metrics.instruments.NBMetricHistogram;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NBMetricHistogramTest {

    @Test
    public void testNicerHistogramValues() {
        final NBMetricHistogram nh = new NBMetricHistogram("testhisto",new DeltaHdrHistogramReservoir("testhisto",4));
        for (int i = 1; 100 >= i; i++) nh.update(i);
        final ConvenientSnapshot snapshot = nh.getSnapshot();
        assertThat(snapshot.getMax()).isEqualTo(100);

        nh.getDeltaSnapshot(500); // Just to reset
        for (int i = 1; 200 >= i; i++ ) nh.update(i);
        final ConvenientSnapshot deltaSnapshot1 = nh.getDeltaSnapshot(500);
        assertThat(deltaSnapshot1.getMax()).isEqualTo(200);

        final ConvenientSnapshot cachedSnapshot = nh.getSnapshot();
        assertThat(cachedSnapshot.getMax()).isEqualTo(200);
        for (int i = 1; 300 >= i; i++ ) nh.update(i);
        final ConvenientSnapshot stillCachedSnapshot = nh.getSnapshot();
        assertThat(stillCachedSnapshot.getMax()).isEqualTo(200);

        try {
            Thread.sleep(501);
        } catch (final InterruptedException ignored) {
        }

        final ConvenientSnapshot notCachedAnyMoreSnapshot = nh.getSnapshot();
        notCachedAnyMoreSnapshot.getMax();
        assertThat(notCachedAnyMoreSnapshot.getMax()).isEqualTo(300);


    }

}
