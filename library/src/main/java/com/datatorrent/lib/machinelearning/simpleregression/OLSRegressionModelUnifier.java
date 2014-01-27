package com.datatorrent.lib.machinelearning.simpleregression;

import com.datatorrent.api.*;
import org.apache.commons.math3.stat.regression.SimpleRegression;

/**
 * Operator to merge the OLS regression models as received from upstream and generate a
 * combined model.
 */
public class OLSRegressionModelUnifier implements Operator.Unifier<SimpleRegression> {

    private SimpleRegression result = new SimpleRegression();

    public transient DefaultOutputPort<SimpleRegression> modelOutputPort = new DefaultOutputPort<SimpleRegression>();

    @Override
    public void setup(Context.OperatorContext operatorContext) {

    }

    @Override
    public void teardown() {

    }

    @Override
    public void beginWindow(long l) {

    }

    @Override
    public void endWindow() {
        modelOutputPort.emit(result);
    }

    @Override
    public void process(SimpleRegression simpleRegression) {
        result.append(simpleRegression);
    }

}
