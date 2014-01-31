package com.datatorrent.lib.machinelearning.timeseries;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.lib.machinelearning.timeseries.linearregression.SLRTimeSeries;

/**
 * Unifier operator to unify the slope calculations made by multiple Slope Operators.
 */
public class SLRTimeSeriesUnifier implements Operator.Unifier<SLRTimeSeries> {

    /**
     * Object to maintain the aggregated state of the data structures computed so far.
     */
    private SLRTimeSeries aggregate = new SLRTimeSeries();

    public transient DefaultOutputPort<SLRTimeSeries> modelOutputPort =
            new DefaultOutputPort<SLRTimeSeries>();

    @Override
    public void setup(Context.OperatorContext operatorContext) {

    }

    @Override
    public void teardown() {

    }

    @Override
    public void beginWindow(long l) {

    }

    @Override
    public void endWindow() {
        aggregate.computeTrendEquation();
        modelOutputPort.emit(aggregate);
    }

    @Override
    public void process(SLRTimeSeries slrTimeSeries) {
        aggregate.append(slrTimeSeries);
    }

}
