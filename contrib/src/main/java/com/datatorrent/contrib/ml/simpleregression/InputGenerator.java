package com.datatorrent.contrib.ml.simpleregression;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class InputGenerator implements InputOperator {

    private static final Logger logger = LoggerFactory.getLogger(InputGenerator.class);
    private int blastCount = 10000;
    private Random random = new Random();

    @OutputPortFieldAnnotation(name = "trainingDataOutput")
    public final transient DefaultOutputPort<TrainingData> trainingDataOutputPort = new DefaultOutputPort<TrainingData>();

    @OutputPortFieldAnnotation(name = "queryDataOutput")
    public final transient DefaultOutputPort<Double> queryOutputPort = new DefaultOutputPort<Double>();


    public void setBlastCount(int blastCount) {
        this.blastCount = blastCount;
    }

    @Override
    public void beginWindow(long windowId) {
    }

    @Override
    public void endWindow() {
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

    @Override
    public void emitTuples() {
        boolean elapsedTimeSent = false;

        for (int i = 0; i < blastCount; ++i) {
            int waitingTime = nextRandomId(3600, 36000);

            double eruptionDuration = -2.15 + 0.05 * waitingTime;
            this.trainingDataOutputPort.emit(new TrainingData(waitingTime, eruptionDuration));

            if (!elapsedTimeSent) {
                if (i % 100 == 0) {
                    this.queryOutputPort.emit(54.0 + waitingTime);
                    elapsedTimeSent = true;
                }
            }
        }
    }

}