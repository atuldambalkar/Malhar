/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.FailedOperationException;
import com.malhartech.dag.ModuleConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes in two streams with identical schema and simply passes on to the output port in order. This module is best suited for inline mode<br>
 * The aim is to simply merge two streams of same schema type<p>
 * <br>
 * <br>
 * Benchmarks: This node has been benchmarked at over 18 million tuples/second in local/inline mode<br>
 *
 * <b>Tuple Schema</b>: All tuples were treated as Object
 * <b>Port Interface</b><br>
 * <b>out_data</b>: Output port for emitting tuples<br>
 * <b>in_data1</b>: Input port for receiving the first stream of incoming tuple<br>
 * <b>in_data2</b>: Input port for receiving the second stream of incoming tuple<br>
 * <br>
 * <b>Properties</b>:
 * None
 * <br>
 * Compile time checks are:<br>
 * no checks are done. Schema check is compile/instantiation time. Not runtime
 * <br>
 *
 * @author amol
 */
@ModuleAnnotation(
        ports = {
  @PortAnnotation(name = StreamMerger.IPORT_IN_DATA1, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = StreamMerger.IPORT_IN_DATA2, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = StreamMerger.OPORT_OUT_DATA, type = PortAnnotation.PortType.OUTPUT)
})
public class StreamMerger extends AbstractModule
{
  public static final String IPORT_IN_DATA1 = "in_data1";
  public static final String IPORT_IN_DATA2 = "in_data2";
  public static final String OPORT_OUT_DATA = "out_data";
  private static Logger LOG = LoggerFactory.getLogger(StreamMerger.class);


  /**
   * Code to be moved to a proper base method name
   *
   * @param config
   * @return boolean
   */
  public boolean myValidation(ModuleConfiguration config)  {
    return true;
  }


  /**
   * Sets up all the config parameters. Assumes checking is done and has passed
   *
   * @param config
   */
  @Override
  public void setup(ModuleConfiguration config) throws FailedOperationException {
    if (!myValidation(config)) {
      throw new FailedOperationException("Did not pass validation");
    }
  }

  /**
   * Process each tuple
   *
   * @param payload
   */
  @Override
  public void process(Object payload) {
    emit(OPORT_OUT_DATA, payload);
  }
}