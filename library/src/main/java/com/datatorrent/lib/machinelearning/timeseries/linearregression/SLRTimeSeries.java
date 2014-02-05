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
package com.datatorrent.lib.machinelearning.timeseries.linearregression;

import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;

/**
 * Time series operator
 *
 * y = Trend * Cyclicality * Seasonality * Irregular factor
 *
 * y = T * C * S * I
 *
 * y / T = S * I - Cyclicality is not considered for this implementation.
 *
 * Determine b1 = (n * Summation (t*y)) - (Summation (t) * Summation (y))
 *                --------------------------------------------------------
 *                (n * Summation (t square)) - ((Summation (t)) square)
 *
 * n = number of training set examples (number of tuples in training set)
 * t = time value 1, 2, 3, 4...n
 *
 * b1 has to have at-least 3 decimal points e.g. 0.944
 *
 * Determine b0 = y bar - (b1 * t bar)
 * y bar = (Summation y) / n
 * t bar = (Summation t) / n
 * Summation n = (n * (n + 1)) / 2
 *
 * Trent T is also called as y hat
 * T = b0 + (b1 * t) - calculate T to the three decimal points for given t value.
 *
 * Calculate the seasonal adjustment and irregularity error -
 * y / T = S * I
 *
 * To predict the quarter value for any n
 * - you calculate the y / T for each time series unit
 * - use this as the deviation or ratio and adjust/multiply it to the value of T calculated above.
 *
 * In order to improve performance it uses only last few cycles of data for Seasonal and Irregularity Error adjustments/smoothing.
 *
 * Reference links -
 * <ul>
 *   <li>http://highered.mcgraw-hill.com/sites/dl/free/0070951640/354829/lind51640_ch16.pdf</li>
 *   <li>Basic Statistics for Business and Economics 3rd Canadian Edition Lind, Marchal, Wathen, and Waite</li>
 *   <li>http://www.youtube.com/watch?v=k9dhcfIyOFc</li>
 *   <li>Lecture Series from Prof. G. Srinivasn, IIT Madras, Forecasting -- Time series models -- Simple Exponential smoothing</li>
 * </ul>
 */
public class SLRTimeSeries {

    /**
     * Number of time intervals in a cycle
     */
    private int timeIntervalsInCycle;

    /**
     * Summation y
     */
    private double sumY;

    /**
     * Summation t
     */
    private double sumT;

    /**
     * Summation t squared
     */
    private double sumTT;

    /**
     * Summation t * y
     */
    private double sumTY;

    /**
     * Total number of time series records.
     */
    private int count;

    /**
     * Intercept value
     */
    private double intercept;

    /**
     * Slope value
     */
    private double slope;

    /**
     * Array to hold seasonal and irregularity adjustment values for each time interval in a cycle.
     */
    private double[] stItAdjustments;

    /**
     * Are Seasonal and Irregularity Error adjustments computed?
     */
    private boolean sIAdjustmentsComputed;

    public SLRTimeSeries() {
    }

    /**
     * Constructor
     *
     * @param timeIntervalsInCycle Number of time intervals in a time series cycle
     */
    public SLRTimeSeries(int timeIntervalsInCycle) {
        this.timeIntervalsInCycle = timeIntervalsInCycle;
        this.stItAdjustments = new double[timeIntervalsInCycle];
    }

    public void setTimeIntervalsInCycle(int timeIntervalsInCycle) {
        this.timeIntervalsInCycle = timeIntervalsInCycle;
    }

    /**
     * Class to capture model for SLR.
     */
    public final static class Model {
        public double intercept;
        public double slope;
    }

    /**
     * Clear all the data.
     */
    public void clear() {
        this.sumY = 0;
        this.sumT = 0;
        this.sumTT = 0;
        this.sumTY = 0;
        this.count = 0;
        this.slope = 0;
        this.intercept = 0;
        this.stItAdjustments = new double[timeIntervalsInCycle];
    }

    public void append(SLRTimeSeries data) {
        this.sumY += data.sumY;
        this.sumT += data.sumT;
        this.sumTT += data.sumTT;
        this.sumTY += data.sumTY;
        this.count += data.count;
    }

    public void process(TimeSeriesData trainingData) {
        this.sumY += trainingData.y;
        double t = calculateTimeInterval(trainingData);
        this.sumT += t;
        this.sumTT += (t * t);
        this.sumTY += (t * trainingData.y);
        this.count++;
    }

    /**
     * Calculate the time interval count for the given training data.
     *
     * @param trainingData
     * @return
     */
    private double calculateTimeInterval(TimeSeriesData trainingData) {
        return trainingData.currentTimeInterval + ((trainingData.currentTimeCycle - 1) * timeIntervalsInCycle);
    }

    /**
     * Calculate the slope =  (n * Summation (t*y)) - (Summation (t) * Summation (y))
     *                            -------------------------------------------------------
     *                            (n * Summation (t square)) - ((Summation (t)) square)
     *

     * @return calculated slope value
     */
    private double calculateSlope() {
        return  ((count * sumTY) - (sumT * sumY))
                        / ((count * sumTT) - (sumT * sumT));
    }

    /**
     * ybar = summation y
     *        -----------
     *            n
     *
     * n is total number of training set data.
     */
    private double getYBar() {
        return sumY / count;
    }

    /**
     * tbar = summation t
     *        -----------
     *            n
     *
     * n is total number of training set data.*
     */
    private double getTBar() {
        return sumT / count;
    }

    /**
     * Compute the values for slope and intercept.
     *
     * Slope =  (n * Summation (t*y)) - (Summation (t) * Summation (y))
     *                            -------------------------------------------------------
     *                            (n * Summation (t square)) - ((Summation (t)) square)
     *
     * ybar = summation y
     *        -----------
     *            n

     * tbar = summation t
     *        -----------
     *            n

     * Intercept =  ybar - (slope * tbar)
     */
    public void computeTrendEquation() {
        // calculate slope value
        this.slope = calculateSlope();

        // calculate ybar and tbar
        double ybar = getYBar();
        double tbar = getTBar();

        // b0 = y bar - (b1 * t bar)
        this.intercept = ybar - (slope * tbar);
    }

    public double getIntercept() {
        return this.intercept;
    }

    public double getSlope() {
        return slope;
    }

    /**
     * Compute the model with latest trend equation and return the computed model.
     * @return Model
     */
    public Model getModel() {

        computeTrendEquation();

        Model model = new Model();
        model.intercept = this.intercept;
        model.slope = this.slope;

        return model;
    }

    /**
     * Predict the value for given time interval.
     *
     * This method calculates the SI adjust
     *
     * @return
     */
    public double predict(int timeInterval) {
        // compute the SI adjustments if not already
        if (!sIAdjustmentsComputed) {

        }

        return (this.intercept + (this.slope * timeInterval)) * stItAdjustments[timeInterval / timeIntervalsInCycle];
    }

}
