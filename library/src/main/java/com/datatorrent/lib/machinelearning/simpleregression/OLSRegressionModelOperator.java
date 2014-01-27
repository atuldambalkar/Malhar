package com.datatorrent.lib.machinelearning.simpleregression;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.ShipContainingJars;
import org.apache.commons.math3.stat.regression.SimpleRegression;


/**
 * Operator to build a Ordinary Least Squares (OLS) Regression Model.
 *
 * It uses using SimpleRegression implementation from Apache Commons Math API.
 */
@ShipContainingJars(classes = {SimpleRegression.class})
public class OLSRegressionModelOperator extends BaseOperator {

    private SimpleRegression simpleRegression = new SimpleRegression();

    public transient DefaultOutputPort<SimpleRegression> modelPort = new DefaultOutputPort<SimpleRegression>() {
        @Override
        public Unifier<SimpleRegression> getUnifier() {
            return new OLSRegressionModelUnifier();
        }
    };

    public transient DefaultInputPort<TrainingData> inputPort = new DefaultInputPort<TrainingData>() {
        @Override
        public void process(TrainingData trainingData) {
            simpleRegression.addData(trainingData.x, trainingData.y);
        }
    };

    @Override
    public void beginWindow(long windowId) {
        simpleRegression.clear();
    }

    @Override
    public void endWindow() {
        modelPort.emit(simpleRegression);
    }
}
