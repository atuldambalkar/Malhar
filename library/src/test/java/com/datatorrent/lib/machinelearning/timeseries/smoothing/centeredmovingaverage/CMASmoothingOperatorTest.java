package com.datatorrent.lib.machinelearning.timeseries.smoothing.centeredmovingaverage;

import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import com.datatorrent.lib.machinelearning.timeseries.smoothing.centeredmovingaverage.CMASmoothingOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test cases for CMASmoothing Operator.
 */
public class CMASmoothingOperatorTest {

    @Test
    public void testEvenTimeIntervals() {

        CMASmoothingOperator oper = new CMASmoothingOperator();
        oper.setNumberOfTimeIntervalsInCycle(4);
        oper.setup(null);
        CollectorTestSink sink = new CollectorTestSink();

        oper.cmaOutputPort.setSink(sink);

        double[][] data = {
                {4.8, 1, 1},
                {4.1, 2, 1},
                {6.0, 3, 1},
                {6.5, 4, 1},
                {5.8, 1, 2},
                {5.2, 2, 2},
                {6.8, 3, 2},
                {7.4, 4, 2},
                {6.0, 1, 3},
                {5.6, 2, 3},
                {7.5, 3, 3},
                {7.8, 4, 3},
                {6.3, 1, 4},
                {5.9, 2, 4},
                {8.0, 3, 4},
                {8.4, 4, 4},
        };
        for (double[] elem: data) {
            TimeSeriesData tuple = new TimeSeriesData();
            tuple.y = elem[0];
            tuple.currentTimeInterval = (int)elem[1];
            tuple.currentTimeCycle = (int)elem[2];
            oper.timeSeriesDataPort.process(tuple);
        }

        oper.endWindow();

        List tuples = sink.collectedTuples;
        Assert.assertEquals(5.4, ((TimeSeriesData)tuples.get(2)).cma, 0.075);
        Assert.assertEquals(5.7, ((TimeSeriesData)tuples.get(3)).cma, 0.037499999999999);
    }

    @Test
    public void testOddTimeIntervals() {

        CMASmoothingOperator oper = new CMASmoothingOperator();
        oper.setNumberOfTimeIntervalsInCycle(5);
        oper.setup(null);
        CollectorTestSink sink = new CollectorTestSink();

        oper.cmaOutputPort.setSink(sink);

        double[][] data = {
                {4.8, 1, 1},
                {4.1, 2, 1},
                {6.0, 3, 1},
                {6.5, 4, 1},
                {7.0, 5, 1},
                {5.8, 1, 2},
                {5.2, 2, 2},
                {6.8, 3, 2},
                {7.4, 4, 2},
                {7.8, 5, 2},
                {6.0, 1, 3},
                {5.6, 2, 3},
                {7.5, 3, 3},
                {7.8, 4, 3},
                {8.3, 5, 3},
                {6.3, 1, 4},
                {5.9, 2, 4},
                {8.0, 3, 4},
                {8.4, 4, 4},
                {8.9, 5, 4},
        };
        for (double[] elem: data) {
            TimeSeriesData tuple = new TimeSeriesData();
            tuple.y = elem[0];
            tuple.currentTimeInterval = (int)elem[1];
            tuple.currentTimeCycle = (int)elem[2];
            oper.timeSeriesDataPort.process(tuple);
        }

        oper.endWindow();

        List tuples = sink.collectedTuples;
    }

}
