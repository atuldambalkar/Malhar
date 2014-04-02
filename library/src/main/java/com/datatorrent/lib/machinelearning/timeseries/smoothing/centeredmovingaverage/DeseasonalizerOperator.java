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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Operator to de-seasonalize the CMA based data tuples within the given application window as received from upstream CMASmoothingOperator. This operator
 * maintains the updated avaerage values for SsubT * IsubT across application windows.
 *
 * Following are steps -
 *
 * 1. Calculate the SsubT and IsubT values for each tuple as,
 *   SsubT * IsubT = YsubT / CMAsubT
 * 2. Get rid of IsubT error component by calculate the average value for each time interval for SsubT * IsubT .
 *    This is done for all the tuples received in the application window, hence calculated in endWindow.
 * 3. Calculate the deseasonalized value (in endWindow) by using SsubT as,
 *   Deseasonalized YsubT = YsubT / SsubT
 *
 * This operator emits the seasonal component by removing the irregularity component. The irregularity component is removed by computing the
 * averages for SsubT * IsubT values for each time interval across all the time-series cycle data sets.
 *
 * Reference links -
 * <ul>
 *   <li>http://www.youtube.com/watch?v=gHdYEZA50KE</li>
 *   <li>Lecture Series from Jaylar Academy</li>
 * </ul>
 *
 */
public class DeseasonalizerOperator extends BaseOperator {

    /**
     * Number of time intervals in a time-series cycle.
     */
    @NotNull
    private int numberOfTimeIntervalsInCycle;

    private SsubTIsubTValue[] stItValues;
    private List<TimeSeriesData> tuples = new ArrayList<TimeSeriesData>();
    private long windowId;

    private static final Logger logger = LoggerFactory.getLogger(DeseasonalizerOperator.class);

    public DeseasonalizerOperator() {
    }

    public DeseasonalizerOperator(int numberOfTimeIntervalsInCycle) {
        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
    }

    public void setNumberOfTimeIntervalsInCycle(int numberOfTimeIntervalsInCycle) {
        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
    }

    @Override
    public void beginWindow(long windowId) {
        this.windowId = windowId;
    }

    @Override
    public void setup(Context.OperatorContext context) {
        this.stItValues = new SsubTIsubTValue[numberOfTimeIntervalsInCycle];
    }

    private final class SsubTIsubTValue {
        double stItSum;
        int timeIntervalsCount;
    }

    public transient DefaultOutputPort<TimeSeriesData> deseasonalizedTimeSeriesPort =
            new DefaultOutputPort<TimeSeriesData>();

    public transient DefaultOutputPort<List<Double>> stItPort = new DefaultOutputPort<List<Double>>();

    /**
     * Calculate SsubT * IsubT component for each tuple.
     */
    public transient DefaultInputPort<TimeSeriesData> cmaSmoothenedTimeSeriesPort = new DefaultInputPort<TimeSeriesData>() {
        @Override
        public void process(TimeSeriesData tuple) {
            if (tuple.cmaCalculatedFlag) {
                tuple.stIt = tuple.y / tuple.cma;
            }
            tuples.add(tuple);
        }
    };

    /**
     * Emit the seasonal component by removing the irregularity component. The irregularity component is removed by computing the
     * averages for SsubT * IsubT values for each time interval across all the time-series cycle data sets.
     */
    @Override
    public void endWindow() {
        // go through the stItList and calculate the averages for each time interval
        if (tuples.size() % numberOfTimeIntervalsInCycle != 0) {
            logger.info("WindowId: " + windowId + ", Model is not ready yet!");
            return;
        }
        printCMATuples(tuples);
        for (TimeSeriesData tuple: tuples) {
            if (!tuple.cmaCalculatedFlag) {
                continue;
            }
            SsubTIsubTValue stItValue = stItValues[tuple.currentTimeInterval - 1];
            if (stItValue == null) {
                SsubTIsubTValue value = new SsubTIsubTValue();
                value.stItSum = tuple.stIt;
                value.timeIntervalsCount++;
                stItValues[tuple.currentTimeInterval - 1] = value;
            } else {
                stItValues[tuple.currentTimeInterval - 1].stItSum += tuple.stIt;  // keep summing
                stItValues[tuple.currentTimeInterval - 1].timeIntervalsCount++;
            }
        }
        // now compute averages for each stIt value in the stItValues array and create a list of averages that
        // can be emitted
        List<Double> stItList = new ArrayList<Double>();
        int index = 0;
        for (SsubTIsubTValue value: stItValues) {
            if (stItValues[index] == null) {
                logger.info("WindowId: " + windowId + ", Model is not ready yet!");
                return;
            }
            stItList.add(value.stItSum / value.timeIntervalsCount);
            index++;
        }
        for (TimeSeriesData tuple: tuples) {
            tuple.deseasonalizedY = tuple.y / stItList.get(tuple.currentTimeInterval - 1);
            deseasonalizedTimeSeriesPort.emit(tuple);
        }
        printStItList(stItList);
        stItPort.emit(stItList);
        tuples.clear();
    }

    private void printStItList(List<Double> list) {
        if (list != null && list.size() > 0) {
            logger.debug("WindowId: " + windowId + ", StItList: " + getAsString(list));
        }
    }

    private String getAsString(List<Double> list) {
        StringBuffer buffer = new StringBuffer();
        if (list != null && list.size() > 0) {
            buffer.append('[');
            for (Double val: list) {
                buffer.append(val).append(',');
            }
            buffer.append(']');
        }
        return buffer.toString();
    }

    private void printCMATuples(List<TimeSeriesData> tuples) {
        logger.debug("WindowId: " + windowId + ", CMA Tuples: " + getTupleValues(tuples));
    }

    private String getTupleValues(List<TimeSeriesData> tuples) {
        StringBuffer buffer = new StringBuffer();
        buffer.append('[');
        for (TimeSeriesData tuple: tuples) {
            buffer.append(tuple.cma).append(',');
        }
        buffer.append(']');
        return buffer.toString();
    }

}
