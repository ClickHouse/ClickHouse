/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.vectorized;

/** ArrowBufBuilder. */
public class MetricsObject {
  public long[] process_time_list;
  public long[] output_length_list;

  public MetricsObject() {}

  /**
   * Create an instance for NativeMetricsObject.
   *
   * @param memoryAddress native ArrowBuf data addr.
   * @param size ArrowBuf size.
   */
  public MetricsObject(long[] _output_length_list, long[] _process_time_list) {
    output_length_list = _output_length_list;
    process_time_list = _process_time_list;
  }
}
