/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.jms;

import javax.jms.Message;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * <p>
 * ActiveMQ input adapter operator with single output port, which consume data
 * from ActiveMQ message bus.
 * <br>
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Have only one output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method getTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 *
 * @param <T>
 * @since 0.3.2
 * @dt-adapter ActiveMQ
 */
public abstract class AbstractActiveMQSinglePortInputOperator<T> extends
		AbstractActiveMQInputOperator
{
	/**
	 * The single output port.
	 */
	@OutputPortFieldAnnotation(name = "outputPort")
	public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

	/**
	 * Any concrete class derived from AbstractActiveMQSinglePortInputOperator has
	 * to implement this method so that it knows what type of message it is going
	 * to send to Malhar. It converts a JMS message into a Tuple. A Tuple can be
	 * of any type (derived from Java Object) that operator user intends to.
	 * 
	 * @param msg
	 * @return newly constructed tuple from the message.
	 */
	public abstract T getTuple(Message msg);

	/**
	 * Implement abstract method.
	 * 
	 * @param msg
	 */
	@Override
	public void emitTuple(Message msg)
	{
		T payload = getTuple(msg);
		if (payload != null) {
			outputPort.emit(payload);
		}
	}
}
