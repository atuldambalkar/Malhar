package com.datatorrent.lib.machinelearning;

/**
 * A forecasting exception to indicate future value can't be forecasted based on the given input model.
 */
public class ForecastingException extends Exception {

    public ForecastingException() {
    }

    public ForecastingException(String message) {
        super(message);
    }

    public ForecastingException(String message, Throwable cause) {
        super(message, cause);
    }
}
