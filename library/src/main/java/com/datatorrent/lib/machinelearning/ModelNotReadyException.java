package com.datatorrent.lib.machinelearning;

/**
 * Exception to indicate that the Machine Learning model is not yet ready to predict the future.
 */
public class ModelNotReadyException extends  Exception {

    public ModelNotReadyException() {
    }

    public ModelNotReadyException(String message) {
        super(message);
    }
}
