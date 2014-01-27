package com.datatorrent.demos.machinelearning.simpleregression;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import org.apache.commons.math3.stat.regression.SimpleRegression;

/**
 * Operator to use unified linear regression model with input query data and return the prediction value.
 */
public class PredictionOperator extends BaseOperator {

    private SimpleRegression model;
    private OutputData outputData = new OutputData();

    public transient DefaultOutputPort<OutputData> predictionPort = new DefaultOutputPort<OutputData>();

    public transient DefaultInputPort<Double> queryPort = new DefaultInputPort<Double>() {
        @Override
        public void process(Double query) {
            if (model != null) {
                outputData.intercept = model.getIntercept();
                outputData.slope = model.getSlope();
                outputData.query = query;
                outputData.prediction = model.predict(query);
                predictionPort.emit(outputData);
            }
        }
    };

    public transient DefaultInputPort<SimpleRegression> modelPort = new DefaultInputPort<SimpleRegression>() {
        @Override
        public void process(SimpleRegression simpleRegression) {
            model = simpleRegression;
        }
    };

}
