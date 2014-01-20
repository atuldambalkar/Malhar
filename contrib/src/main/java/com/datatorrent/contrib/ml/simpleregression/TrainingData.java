package com.datatorrent.contrib.ml.simpleregression;

/**
 * Training data class to capture Old-Faithful training data.
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
}
