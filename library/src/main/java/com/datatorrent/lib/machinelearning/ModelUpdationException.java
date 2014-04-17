package com.datatorrent.lib.machinelearning;

/**
 * Exception to indicate that the Machine Learning model can not be updated.
 */
public class ModelUpdationException extends  Exception {

    public ModelUpdationException() {
    }

    public ModelUpdationException(String message) {
        super(message);
    }
}
