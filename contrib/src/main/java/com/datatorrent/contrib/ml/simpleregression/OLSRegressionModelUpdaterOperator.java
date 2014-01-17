package com.datatorrent.contrib.ml.simpleregression;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import org.apache.commons.math3.stat.regression.SimpleRegression;

/**
 * Operator to merge the OLS regression models as received from upstream and generate a
 * combined model.
 */
public class OLSRegressionModelUpdaterOperator extends BaseOperator {

    private SimpleRegression result = new SimpleRegression();
    private double query;

    public transient DefaultOutputPort<Double> outputPort = new DefaultOutputPort<Double>();

    public transient DefaultInputPort<SimpleRegression> simpleRegressionInputPort = new DefaultInputPort<SimpleRegression>() {
        @Override
        public void process(SimpleRegression simpleRegression) {
            result.append(simpleRegression);
        }
    };

    public transient DefaultInputPort<Double> queryPort = new DefaultInputPort<Double>() {
        @Override
        public void process(Double queryInput) {
            query = queryInput;
        }
    };

    @Override
    public void endWindow() {
        if (result.getN() > 0) {
            outputPort.emit(result.predict(query));
        }
    }
}
