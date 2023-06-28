package io.nosqlbench.virtdata.library.basics.shared.vectors.algorithms;

import java.util.List;

public interface CircleAlgorithm {
    public List<Object> getVector(long value, long circleCount);

    public double getMinimumVectorAngle();
}
