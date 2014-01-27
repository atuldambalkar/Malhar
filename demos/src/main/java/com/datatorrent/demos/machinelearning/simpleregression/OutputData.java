package com.datatorrent.demos.machinelearning.simpleregression;

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
        return String.format("Intercept: %f, Slope: %f, Queried: %f, Prediction: %f", intercept, slope, query, prediction);
//        return String.format("Query home size: %f (Sq. Feet), Predicted home price: %f (K USD)", query, prediction);
    }
}
