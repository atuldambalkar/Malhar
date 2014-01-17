package com.datatorrent.contrib.ml.simpleregression;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import org.apache.commons.math3.stat.regression.SimpleRegression;


/**
 * Operator to build a Ordinary Least Squares (OLS) Regression Model.
 *
 * It uses using SimpleRegression implementation from Apache Commons Math API.
 */
public class OLSRegressionModelOperator extends BaseOperator {

    private SimpleRegression simpleRegression = new SimpleRegression();

    public transient DefaultOutputPort<SimpleRegression> simpleRegressionOutputPort = new DefaultOutputPort<SimpleRegression>();

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
        SimpleRegression obj = new SimpleRegression();
        obj.append(simpleRegression);
        simpleRegressionOutputPort.emit(obj);
    }
}
