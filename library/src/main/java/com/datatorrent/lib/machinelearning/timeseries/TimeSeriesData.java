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
package com.datatorrent.lib.machinelearning.timeseries;

/**
 * Class to represent TimeSeries data.
 *
 */
public class TimeSeriesData {

    /**
     * Data value for the given time interval
     */
    public double y;

    /**
     * Current time interval. For example, If there are 4 quarters (1, 2, 3, 4) in a year, then this value indicates
     * the quarter number which will get incremented by 1 every time a quarter is completed.
     * So if the training data represents data for 2nd quarter, then this value will be 2.
     */
    public int currentTimeInterval;  // t

    /**
     * Current time cycle. For example, If there are 4 quarters (1, 2, 3, 4) in a year, then 1 year represents a cycle.
     * So, this value indicates the current time cycle and will get incremented by 1 every time a cycle is completed.
     * So if the training data represents 3rd year since we started monitoring the data, then the value for this
     * variable will be 3.
     */
    public int currentTimeCycle;

    /**
     * Center Moving Average for the underlying time series cycles.
     */
    public double cma;

}
