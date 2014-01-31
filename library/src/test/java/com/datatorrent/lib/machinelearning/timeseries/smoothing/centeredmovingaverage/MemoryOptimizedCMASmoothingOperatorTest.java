/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.machinelearning.timeseries.smoothing.centeredmovingaverage;

import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Test cases for CMASmoothing Operator.
 */
public class MemoryOptimizedCMASmoothingOperatorTest {

    @Test
    public void testEvenTimeIntervals() {

        MemoryOptimizedCMASmoothingOperator oper = new MemoryOptimizedCMASmoothingOperator();
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
        Assert.assertEquals(5.4, ((TimeSeriesData) tuples.get(2)).cma, 0.075);
        Assert.assertEquals(5.7, ((TimeSeriesData)tuples.get(3)).cma, 0.037499999999999);

    }

    @Test
    public void testOddTimeIntervals() {

        MemoryOptimizedCMASmoothingOperator oper = new MemoryOptimizedCMASmoothingOperator();
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
