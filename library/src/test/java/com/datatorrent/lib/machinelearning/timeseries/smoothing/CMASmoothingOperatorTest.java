package com.datatorrent.lib.machinelearning.timeseries.smoothing;

import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import com.datatorrent.lib.machinelearning.timeseries.smoothing.centeredmovingaverage.CMASmoothingOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import org.junit.Test;

import java.util.List;

/**
 * Test cases for CMASmoothing Operator.
 */
public class CMASmoothingOperatorTest {

    @Test
    public void testOddTimeIntervals() {

        CMASmoothingOperator oper = new CMASmoothingOperator(4);
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
    }
}
