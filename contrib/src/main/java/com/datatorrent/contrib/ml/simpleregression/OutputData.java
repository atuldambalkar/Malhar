package com.datatorrent.contrib.ml.simpleregression;

/**
 * Output result data
 *
 */
public class OutputData {

    public double query;
    public double prediction;
    public double intercept;
    public double slope;

    @Override
    public String toString() {
        return String.format("Intercept: %f, Slope: %f, Query: %f, Prediction: %f", intercept, slope, query, prediction);
    }
}
