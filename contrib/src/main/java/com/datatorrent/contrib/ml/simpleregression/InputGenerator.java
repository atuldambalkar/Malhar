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

        for (int i = 0; i < blastCount; ++i) {
            int size = nextRandomId(500, 3000);

            double price = 1.3 + 1.1 * size;
            this.trainingDataOutputPort.emit(new TrainingData(size, price));

            if (i % 1000 == 0) {
                this.queryOutputPort.emit(100.0 + size);
            }
        }
    }

}