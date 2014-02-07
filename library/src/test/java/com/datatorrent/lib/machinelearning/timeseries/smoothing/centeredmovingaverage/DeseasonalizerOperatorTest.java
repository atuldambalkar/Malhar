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
import org.junit.Test;

import java.util.List;

/**
 * Test cases for Deseasonalizer operator.
 */
public class DeseasonalizerOperatorTest {

    @Test
    public void testEvenDesosonalizer() {

        DeseasonalizerOperator oper = new DeseasonalizerOperator();
        oper.setNumberOfTimeIntervalsInCycle(4);
        oper.setup(null);

        CollectorTestSink sink = new CollectorTestSink();

        oper.deseasonalizedTimeSeriesPort.setSink(sink);

        double[][] data = {
                {4.8, 1, 1, 0},
                {4.1, 2, 1, 0},
                {6.0, 3, 1, 5.475},
                {6.5, 4, 1, 5.737},
                {5.8, 1, 2, 5.975},
                {5.2, 2, 2, 6.187},
                {6.8, 3, 2, 6.324},
                {7.4, 4, 2, 6.399},
                {6.0, 1, 3, 6.537},
                {5.6, 2, 3, 6.674},
                {7.5, 3, 3, 6.762},
                {7.8, 4, 3, 6.837},
                {6.3, 1, 4, 6.937},
                {5.9, 2, 4, 7.074},
                {8.0, 3, 4, 0},
                {8.4, 4, 4, 0},
        };

        for (double[] elem: data) {
            TimeSeriesData tuple = new TimeSeriesData();
            tuple.y = elem[0];
            tuple.currentTimeInterval = (int)elem[1];
            tuple.currentTimeCycle = (int)elem[2];
            tuple.cma = elem[3];
            tuple.cmaCalculatedFlag = elem[3] > 0;
            oper.cmaSmoothenedTimeSeriesPort.process(tuple);
        }
        oper.endWindow();

        List tuple = sink.collectedTuples;

    }

    @Test
    public void testOddDesosonalizer() {

        DeseasonalizerOperator oper = new DeseasonalizerOperator();
        oper.setNumberOfTimeIntervalsInCycle(5);
        oper.setup(null);

        CollectorTestSink sink = new CollectorTestSink();

        oper.deseasonalizedTimeSeriesPort.setSink(sink);

        double[][] data = {
                {4.8,	1,	1,	0},
                {4.1,	2,	1,	0},
                {6.0,	3,	1,	5.68},
                {6.5,	4,	1,	5.88},
                {7.0,	5,	1,	6.1},
                {5.8,	1,	2,	6.26},
                {5.2,	2,	2,	6.44},
                {6.8,	3,	2,	6.6},
                {7.4,	4,	2,	6.64},
                {7.8,	5,	2,	6.72},
                {6.0,	1,	3,	6.86},
                {5.6,	2,	3,	6.94},
                {7.5,	3,	3,	7.04},
                {7.8,	4,	3,	7.1},
                {8.3,	5,	3,	7.16},
                {6.3,	1,	4,	7.26},
                {5.9,	2,	4,	7.38},
                {8.0,	3,	4,	7.5},
                {8.4,	4,	4,	0},
                {8.9,	5,	4,	0}
        };

        for (double[] elem: data) {
            TimeSeriesData tuple = new TimeSeriesData();
            tuple.y = elem[0];
            tuple.currentTimeInterval = (int)elem[1];
            tuple.currentTimeCycle = (int)elem[2];
            tuple.cma = elem[3];
            tuple.cmaCalculatedFlag = elem[3] > 0;
            oper.cmaSmoothenedTimeSeriesPort.process(tuple);
        }
        oper.endWindow();

        List tuple = sink.collectedTuples;

    }
}
