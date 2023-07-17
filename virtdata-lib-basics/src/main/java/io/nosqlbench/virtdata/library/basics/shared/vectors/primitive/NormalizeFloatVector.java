/*
 * Copyright (c) 2023 nosqlbench
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

package io.nosqlbench.virtdata.library.basics.shared.vectors.primitive;

import io.nosqlbench.virtdata.api.annotations.Categories;
import io.nosqlbench.virtdata.api.annotations.Category;
import io.nosqlbench.virtdata.api.annotations.ThreadSafeMapper;

import java.util.function.Function;

@ThreadSafeMapper
@Categories(Category.experimental)
public class NormalizeFloatVector implements Function<float[],float[]> {

    @Override
    public float[] apply(float[] floats) {
        float[] normalized = new float[floats.length];
        double accumulator = 0.0d;
        for (int i = 0; i < floats.length; i++) {
            accumulator+=floats[i]*floats[i];
        }
        double scale = Math.sqrt(accumulator);
        for (int i = 0; i < floats.length; i++) {
            normalized[i]=(float)(floats[i]/scale);
        }
        return normalized;
    }
}
