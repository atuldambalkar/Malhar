package com.datatorrent.lib.machinelearning.simpleregression;

import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.junit.Test;

import java.util.List;

/**
 * Test class for OLSRegressionModel Operator
 */
public class OLSRegressionModelOperatorTest {

    @Test
    public void testOLSRegression() {
        double[][] data = new double[][] {
                {1.0, 1.0},
                {2.0, 2.0},
                {3.0, 3.0},
                {4.0, 4.0},
                {5.0, 5.0},
                {6.0, 6.0},
                {7.0, 7.0},
                {8.0, 8.0},
        };

        OLSRegressionModelOperator operator = new OLSRegressionModelOperator();
        CollectorTestSink modelSink = new CollectorTestSink();
        operator.modelPort.setSink(modelSink);

        for (double[] tuple: data) {
            operator.inputPort.process(new TrainingData(tuple[0], tuple[1]));
        }

        operator.endWindow();

        List tuples = modelSink.collectedTuples;
        SimpleRegression simpleRegression = (SimpleRegression)tuples.get(0);
        Assert.assertEquals(simpleRegression.getIntercept(), 0.0);
        Assert.assertEquals(simpleRegression.getSlope(), 1.0);
    }
}
