package com.datatorrent.lib.machinelearning.timeseries.smoothing.centeredmovingaverage;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

/**
 * Operator to deseasonalize the CMA based data tuples within the given application window as received from upstream CMASmoothingOperator.
 *
 * Following are steps -
 *
 * 1. Calculate the SsubT and IsubT values for each tuple as,
 *   SsubT * IsubT = YsubT / CMAsubT
 * 2. Get rid of IsubT error component by calcualte teh average value for each time interval for SsubT * IsubT .
 *    This is done for all the tuples received in the application window, hence calculated in endWindow.
 * 3. Calculate the deseasonalized value (in endWindow) by using SsubT as,
 *   Deseasonalized YsubT = YsubT / SsubT
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

    public DeseasonalizerOperator() {
    }

    public DeseasonalizerOperator(int numberOfTimeIntervalsInCycle) {
        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
    }

    public void setNumberOfTimeIntervalsInCycle(int numberOfTimeIntervalsInCycle) {
        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
    }

    @Override
    public void setup(Context.OperatorContext context) {
        this.stItValues = new SsubTIsubTValue[numberOfTimeIntervalsInCycle];
    }

    private final class SsubTIsubTValue {
        double stItSum;
        int timeIntervalsCount;
//        double stItAverage;
    }

    public transient DefaultOutputPort<TimeSeriesData> deseasonalizedTimeSeriesPort =
            new DefaultOutputPort<TimeSeriesData>();

    public transient DefaultOutputPort<List<Double>> stItPort = new DefaultOutputPort<List<Double>>();

    /**
     * Calculate SsubT * IsubT component for each tuple.
     */
    public transient DefaultInputPort<TimeSeriesData> cmaTimeSeriesPort = new DefaultInputPort<TimeSeriesData>() {
        @Override
        public void process(TimeSeriesData tuple) {
            if (tuple.cmaCalculatedFlag) {
                tuple.stIt = tuple.y / tuple.cma;
            }
            tuples.add(tuple);
        }
    };

    @Override
    public void endWindow() {
        // go through the stItList and calculate the averages for each time interval
        if (tuples.size() % numberOfTimeIntervalsInCycle != 0) {
            throw new IllegalStateException("Incorrect number of records (" + tuples.size() + ") received. " +
                    "Need to match multiple of time intervals (" + numberOfTimeIntervalsInCycle + ") in a time series cycle: ");
        }
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
                throw new IllegalStateException("Insufficient data supplied. Data for not all time intervals is available!");
            }
            stItList.add(value.stItSum / value.timeIntervalsCount);
            index++;
        }
        for (TimeSeriesData tuple: tuples) {
            tuple.deseasonalizedY = tuple.y / stItList.get(tuple.currentTimeInterval - 1);
            deseasonalizedTimeSeriesPort.emit(tuple);
        }
        stItPort.emit(stItList);
    }
}
