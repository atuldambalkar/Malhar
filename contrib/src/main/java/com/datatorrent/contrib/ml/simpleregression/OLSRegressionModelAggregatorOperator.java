package com.datatorrent.contrib.ml.simpleregression;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import org.apache.commons.math3.stat.regression.SimpleRegression;

/**
 * Operator to merge the OLS regression models as received from upstream and generate a
 * combined model to be passed to downstream operator.
 */
public class OLSRegressionModelAggregatorOperator extends BaseOperator {

    private SimpleRegression result = new SimpleRegression();

    public transient DefaultOutputPort<SimpleRegression> aggregatedModelOutputPort = new DefaultOutputPort<SimpleRegression>();

    public transient DefaultInputPort<SimpleRegression> simpleRegressionInputPort = new DefaultInputPort<SimpleRegression>() {
        @Override
        public void process(SimpleRegression simpleRegression) {
            result.append(simpleRegression);
        }
    };

    @Override
    public void endWindow() {
        aggregatedModelOutputPort.emit(result);
    }
}
