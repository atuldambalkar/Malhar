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

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import javax.validation.constraints.NotNull;

/**
 * Seasonality and Irregularity Error smoothing operator based on Centered Moving Averaging Technique.
 *
 * This operator will calculate the CMA for the incoming time-series data and
 * emit the updated tuple with corresponding CMA value.
 *
 * Important Note: The application window for this Operator needs to cover exact number of data inputs
 * that complete any arbitrary 'n' number of complete time-series cycles.
 *
 * TODO: Explore if this can be turned into SlidingWindow operator
 */
public class MemoryOptimizedCMASmoothingOperator extends BaseOperator {

    /**
     * Number of time intervals in a time-series cycle.
     */
    @NotNull
    private int numberOfTimeIntervalsInCycle;

    private CircularFifoBuffer circularYBuffer;

    private CircularFifoBuffer circularTupleBuffer;

    private boolean firstCycle = true;
    private double sum;
    private double lastMA;
    private boolean calculateAverage;
    private boolean evenIntervalsInCycle;

    public transient DefaultOutputPort<TimeSeriesData> cmaOutputPort = new DefaultOutputPort<TimeSeriesData>();

    public MemoryOptimizedCMASmoothingOperator() {
    }

    public void setNumberOfTimeIntervalsInCycle(int numberOfTimeIntervalsInCycle) {
        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
    }

    @Override
    public void setup(Context.OperatorContext context) {
        this.circularYBuffer = new CircularFifoBuffer(numberOfTimeIntervalsInCycle);
        this.circularTupleBuffer = new CircularFifoBuffer(numberOfTimeIntervalsInCycle);
        this.evenIntervalsInCycle = numberOfTimeIntervalsInCycle % 2 == 0;
    }

    public transient DefaultInputPort<TimeSeriesData> timeSeriesDataPort = new DefaultInputPort<TimeSeriesData>() {
        @Override
        public void process(TimeSeriesData data) {
            circularTupleBuffer.add(data);
            if (firstCycle) {   // we are yet to get all the data points in the first cycle
                sum += data.y;  // so keep summing
                if (data.currentTimeInterval == numberOfTimeIntervalsInCycle) {
                    double ma = sum / numberOfTimeIntervalsInCycle;
                    processTupleBuffer(ma);
                    firstCycle = false; // first cycle is complete
                }
            } else {
                // update the tuple list with the CMA value
                sum -= (Double)circularYBuffer.remove(); // remove the first element from the circular buffer
                sum += data.y;  // add the latest value
                double ma = sum / numberOfTimeIntervalsInCycle;
                processTupleBuffer(ma);
            }
            circularYBuffer.add(data.y);
        }
    };

    /**
     * Update the tuple list based on if the number of intervals in a cycle is odd or even.
     *
     * In case of odd, the SMA is itself CMA.
     * But in case of even, it needs to be average of two consecutive SMAs and use that value as CMA.
     *
     * @param ma
     */
    private void processTupleBuffer(double ma) {
        if (firstCycle) {
            int removalCount = numberOfTimeIntervalsInCycle / 2;
            while (removalCount > 0) {
                cmaOutputPort.emit((TimeSeriesData) circularTupleBuffer.remove());
                removalCount--;
            }
        }
        if (evenIntervalsInCycle) {
            if (!calculateAverage) {
                calculateAverage = true;
                this.lastMA = ma;
                return;
            }
            double avgMA = (ma + this.lastMA) / 2;
            this.lastMA = ma;
            ma = avgMA;
        }
        TimeSeriesData data = (TimeSeriesData) circularTupleBuffer.remove();
        data.cma = ma;
        cmaOutputPort.emit(data);
    }

    /**
     * Just emit the remaining objects that don't have cma calculated as is.
     */
    @Override
    public void endWindow() {
        for (Object tuple: circularTupleBuffer) {
            cmaOutputPort.emit((TimeSeriesData)tuple);
        }
        circularTupleBuffer.clear();
    }


}
