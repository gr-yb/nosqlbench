package io.nosqlbench.virtdata.library.basics.shared.vectors;

import io.nosqlbench.virtdata.api.annotations.Categories;
import io.nosqlbench.virtdata.api.annotations.Category;
import io.nosqlbench.virtdata.api.annotations.ThreadSafeMapper;
import io.nosqlbench.virtdata.library.basics.shared.vectors.algorithms.CircleAlgorithm;

import java.util.List;
import java.util.function.LongFunction;

@Categories(Category.general)
@ThreadSafeMapper
public class CircleVectors implements LongFunction<List<Object>> {
    private final int circleCount;
    private final CircleAlgorithm algorithm;

    public CircleVectors(int circleCount, String algorithmClass) throws Exception {
        this.circleCount = circleCount;
        Object algo = Class.forName(algorithmClass).newInstance();
        if (!(algo instanceof CircleAlgorithm)) {
            throw new RuntimeException("The class '" + algorithmClass +
                "' does not implement CircleAlgorithm");
        }
        algorithm = (CircleAlgorithm) algo;
    }

    @Override
    public List<Object> apply(long value) {
        return algorithm.getVector((value % circleCount), circleCount);
    }
}
