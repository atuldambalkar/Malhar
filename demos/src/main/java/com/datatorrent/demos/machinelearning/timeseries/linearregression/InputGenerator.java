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
package com.datatorrent.demos.machinelearning.timeseries.linearregression;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * Generate the training data tuples for time series data.
 *
 * The input generator will generate call rate data at one hour time interval. It will generate data for one time series cycle which is for 24 hours.
 *
 * Also generate query data.
 */
public class InputGenerator implements InputOperator {

    private static final Logger logger = LoggerFactory.getLogger(InputGenerator.class);
    private boolean trainingDataGenerated;
    private int timeInterval = 1;
    private int currentTimeCycle = 1;
    private Random random = new Random();

    private int[][] hourlyCallRateRanges = {
            {5, 10},   // hour 1 00.00 to 1.00am
            {7, 15},   // hour 2 1.00 to 2.00am
            {3, 5},   // hour 3 2.00 to 3.00am
            {5, 10},   // hour 4 3.00 to 4.00am
            {5, 15},   // hour 5 4.00 to 5.00am
            {15, 20},   // hour 6 5.00 to 6.00am
            {20, 30},   // hour 7 6.00 to 7.00am
            {25, 35},   // hour 8 7.00 to 8.00am
            {25, 40},   // hour 9 8.00 to 9.00am
            {50, 100},   // hour 10 9.00 to 10.00am
            {60, 120},   // hour 11 10.00 to 11.00am
            {80, 150},   // hour 12 11.00 to 12.00am
            {60, 130},   // hour 13 12.00 to 13.00am
            {55, 120},   // hour 14 13.00 to 14.00am
            {50, 110},   // hour 15 14.00 to 15.00am
            {50, 110},   // hour 16 15.00 to 16.00am
            {50, 100},   // hour 17 16.00 to 17.00am
            {40, 90},   // hour 18 17.00 to 18.00am
            {40, 80},   // hour 19 18.00 to 19.00am
            {35, 60},   // hour 20 19.00 to 20.00am
            {30, 50},   // hour 21 20.00 to 21.00am
            {10, 20},   // hour 22 21.00 to 22.00am
            {5, 15},   // hour 23 22.00 to 23.00am
            {5, 10},   // hour 24 23.00 to 24.00am
    };

    @OutputPortFieldAnnotation(name = "trainingDataOutput")
    public final transient DefaultOutputPort<TimeSeriesData> timeSeriesDataOutputPort = new DefaultOutputPort<TimeSeriesData>();

    @OutputPortFieldAnnotation(name = "queryDataOutput")
    public final transient DefaultOutputPort<Integer> queryOutputPort = new DefaultOutputPort<Integer>();

    @Override
    public void beginWindow(long windowId) {
    }

    @Override
    public void endWindow() {
        trainingDataGenerated = false; // set to false so that on next window, the training data will be generated
        timeInterval = 1;
    }

    @Override
    public void setup(Context.OperatorContext context) {
    }

    @Override
    public void teardown() {
    }

    private int nextRandomId(int min, int max) {
        int id;
        do {
            id = (int) Math.abs(Math.round(random.nextGaussian() * max / 2));
        }
        while (id >= max);

        if (id < min) {
            id += min;
        }
        return id;
    }

    /**
     * Each streaming window will generate data for one time series cycle.
     */
    @Override
    public void emitTuples() {
        if (!trainingDataGenerated) {
            int count = 0;
            while (count < 5) {
                int timeInterval = 1;
                for (int[] hourlyCallRateRange: hourlyCallRateRanges) {
                    TimeSeriesData tuple = new TimeSeriesData();
                    tuple.y = nextRandomId(hourlyCallRateRange[0], hourlyCallRateRange[1]);
                    tuple.currentTimeInterval = timeInterval;
                    tuple.currentTimeCycle = currentTimeCycle;
                    timeInterval++;
                    timeSeriesDataOutputPort.emit(tuple);
                }
                currentTimeCycle++;
                if (currentTimeCycle % 5 == 0) {  // sufficient data is now generated. So generate the query input
                    queryOutputPort.emit((currentTimeCycle * 24) + nextRandomId(1, 24));
                }
                count++;
            }
        }
        trainingDataGenerated = true;
    }

}