package com.datatorrent.lib.machinelearning.timeseries.smoothing.centeredmovingaverage;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.machinelearning.timeseries.TimeSeriesData;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

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
public class CMASmoothingOperator extends BaseOperator {

    /**
     * Number of time intervals in a time-series cycle.
     */
    @NotNull
    private int numberOfTimeIntervalsInCycle;

    private CircularFifoBuffer circularYBuffer;

    private List<TimeSeriesData> tupleList = new ArrayList<TimeSeriesData>();

    private boolean firstCycle = true;
    private double sum;
    private int emitListIndex;
    private double lastMA;
    private boolean calculateAverage;
    private boolean evenIntervalsInCycle;

    public transient DefaultOutputPort<TimeSeriesData> cmaOutputPort = new DefaultOutputPort<TimeSeriesData>();

    public CMASmoothingOperator() {
    }

//    public CMASmoothingOperator(int numberOfTimeIntervalsInCycle) {
//        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
//        this.circularYBuffer = new CircularFifoBuffer(numberOfTimeIntervalsInCycle);
//        this.emitListIndex = numberOfTimeIntervalsInCycle / 2;
//        this.evenIntervalsInCycle = numberOfTimeIntervalsInCycle % 2 == 0;
//    }


    public void setNumberOfTimeIntervalsInCycle(int numberOfTimeIntervalsInCycle) {
        this.numberOfTimeIntervalsInCycle = numberOfTimeIntervalsInCycle;
    }

    @Override
    public void setup(Context.OperatorContext context) {
        this.circularYBuffer = new CircularFifoBuffer(numberOfTimeIntervalsInCycle);
        this.emitListIndex = numberOfTimeIntervalsInCycle / 2;
        this.evenIntervalsInCycle = numberOfTimeIntervalsInCycle % 2 == 0;
    }

    public transient DefaultInputPort<TimeSeriesData> timeSeriesDataPort = new DefaultInputPort<TimeSeriesData>() {
        @Override
        public void process(TimeSeriesData data) {
            tupleList.add(data);
            if (firstCycle) {   // we are yet to get all the data points in the first cycle
                sum += data.y;  // so keep summing
                if (data.currentTimeInterval == numberOfTimeIntervalsInCycle) {
                    firstCycle = false; // first cycle is complete
                    double ma = sum / numberOfTimeIntervalsInCycle;
                    updateTupleList(ma);
                }
            } else {
                // update the tuple list with the CMA value
                sum -= (Double)circularYBuffer.remove(); // remove the first element from the circular buffer
                sum += data.y;  // add the latest value
                double ma = sum / numberOfTimeIntervalsInCycle;
                updateTupleList(ma);
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
    private void updateTupleList(double ma) {
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
        TimeSeriesData tuple = tupleList.get(emitListIndex);
        tuple.cma = ma;
        tuple.cmaCalculatedFlag = true;
        emitListIndex++;
    }

    @Override
    public void endWindow() {
        for (TimeSeriesData tuple: tupleList) {
            cmaOutputPort.emit(tuple);
        }
    }
}
