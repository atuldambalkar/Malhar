/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.ads_dimension;

import com.datatorrent.lib.io.SimpleSinglePortInputOperator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class AdsDimensionLogInputOperator extends SimpleSinglePortInputOperator<Map<String, Object>> implements Runnable
{
  private transient AtomicInteger lineCount = new AtomicInteger();
  private int serverPort = 4444;
  private static final Logger LOG = LoggerFactory.getLogger(AdsDimensionLogInputOperator.class);

  public void setServerPort(int port)
  {
    this.serverPort = port;
  }

  public static Map<String, Object> parseLogLine(String line)
  {
    Map<String, Object> parsedData = new HashMap<String, Object>();

    String[] data = line.split("\t\\|");
    for (String dataPair: data) {
      String[] keyValue = dataPair.split("=");
      if (keyValue.length == 0) {
        continue;
      }
      if (keyValue.length == 2) {
        parsedData.put(keyValue[0], keyValue[1]);
      }
      else if (keyValue.length == 1) {
        parsedData.put(keyValue[0], "");
      }
      else {
        parsedData.put(keyValue[0],
                       dataPair.substring(dataPair.indexOf('=') + 1));
      }
    }

    return parsedData;
  }

  @Override
  public void endWindow()
  {
    System.out.println("Number of log lines: " + lineCount);
    lineCount.set(0);
  }

  @Override
  public void run()
  {
    try {
      ServerSocket serverSocket = new ServerSocket(serverPort);
      while (true) {
        Socket clientSocket = serverSocket.accept();

        BufferedReader br = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
        String line;
        int lineno = 0;
        while ((line = br.readLine()) != null) {
          ++lineno;
          Map<String, Object> map = parseLogLine(line);
          map.put("lineno", lineno);
          this.outputPort.emit(map);
          Thread.sleep(1);
          lineCount.incrementAndGet();
          //System.out.println("Line count : " + lineCount);
        }
        System.out.println("LOG HAS ENDED!");
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}
