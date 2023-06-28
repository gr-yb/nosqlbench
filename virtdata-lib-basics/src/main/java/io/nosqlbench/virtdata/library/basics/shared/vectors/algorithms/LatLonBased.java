package io.nosqlbench.virtdata.library.basics.shared.vectors.algorithms;

import java.util.List;

public class LatLonBased implements CircleAlgorithm {
    private final static double goldenAngle = 137.5;

    @Override
    public List<Object> getVector(long value, long circleCount) {
        double longitude = 2 * Math.PI * value / circleCount;
        double latitude = Math.asin(1 - 2 * (double) value / (circleCount - 1));
        double x = Math.cos(latitude) * Math.cos(longitude);
        double y = Math.cos(latitude) * Math.sin(longitude);
        double z = Math.sin(latitude);

        return List.of((float)x, (float)y, (float)z);
    }

    @Override
    public double getMinimumVectorAngle() {
        return 0;
    }
}
