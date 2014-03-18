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

    public ModelNotReadyException(String message, Throwable cause) {
        super(message, cause);
    }

    public ModelNotReadyException(Throwable cause) {
        super(cause);
    }

    public ModelNotReadyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
