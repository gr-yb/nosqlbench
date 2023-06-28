package io.nosqlbench.virtdata.library.basics.shared.vectors.algorithms;

import java.util.List;

public class GoldenAngle implements CircleAlgorithm {

    private final static double goldenAngle = 137.5;

    @Override
    public List<Object> getVector(long value, long circleCount) {
        double y = 1 - (value / (double) (circleCount - 1)) * 2;
        double radius = Math.sqrt(1 - y * y);
        double theta = goldenAngle * value;
        double x = Math.cos(theta) * radius;
        double z = Math.sin(theta) * radius;

        return List.of((float)x, (float)y, (float)z);
    }

    @Override
    public double getMinimumVectorAngle() {
        return 0;
    }
}
