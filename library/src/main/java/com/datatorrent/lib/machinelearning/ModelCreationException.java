package com.datatorrent.lib.machinelearning;

/**
 * Exception to indicate that the Machine Learning model is not yet ready to predict the future.
 */
public class ModelCreationException extends  Exception {

    public ModelCreationException() {
    }

    public ModelCreationException(String message) {
        super(message);
    }
}
