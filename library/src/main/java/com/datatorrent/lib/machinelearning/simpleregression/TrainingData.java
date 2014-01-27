package com.datatorrent.lib.machinelearning.simpleregression;

/**
 * Training data class to capture univariate training data.
 */
public class TrainingData {

    public double x;
    public double y;

    public TrainingData() {
    }

    public TrainingData(double x, double y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public int hashCode() {
        return (int)(x * y);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TrainingData) {
            TrainingData obj1 = (TrainingData)obj;
            return this.x == obj1.x && this.y == obj1.y;
        }
        return false;
    }
}
