/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.entity

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import spray.json._
import org.apache.openwhisk.core.ConfigKeys
import pureconfig._
import pureconfig.generic.auto._

case class CPULimitConfig(controlEnabled: Boolean, min: Float, max: Float, std: Float)

/**
 * CPULimit encapsulates allowed CPU for an action. The limit must be within a
 * permissible range (by default [0.1, 1.0]).
 *
 * A value type (hence == is .equals, immutable and cannot be null).
 * The constructor is private, for checking and normalizing argument
 * before creating a new instance.
 *
 * @param threads the CPU utilisation limit in cpuThreads for the action
 */
protected[entity] class CPULimit private (val threads: Float) extends AnyVal

protected[core] object CPULimit extends ArgNormalizer[CPULimit] {
  val config = loadConfigOrThrow[CPULimitConfig](ConfigKeys.cpu)

  /** static value */
  protected[core] val CPU_LIMIT_ENABLED: Boolean = config.controlEnabled
  protected[core] val MIN_CPU: Float = config.min
  protected[core] val MAX_CPU: Float = config.max
  protected[core] val STD_CPU: Float = config.std

  /** A singleton CPULimit with default value */
  protected[core] val standardCPULimit = CPULimit(STD_CPU)

  protected[core] def apply(): CPULimit = standardCPULimit

  /**
   * Creates CPULimit for limit, iff limit is within permissible range.
   *
   * @param  the limit in CPU threads, must be within permissible range
   * @return CPULimit with limit set
   * @throws IllegalArgumentException if limit does not conform to requirements
   */
  @throws[IllegalArgumentException]
  protected[core] def apply(threads: Float): CPULimit = {
    require(threads >= MIN_CPU, s"CPU $threads below allowed threshold of $MIN_CPU")
    require(threads <= MAX_CPU, s"CPU $threads exceeds allowed threshold of $MAX_CPU")
    new CPULimit(threads)
  }

  /**
   * Creates CPULimit for limit, iff limit is within permissible range.
   *
   * @param  the limit in CPU threads, must be within permissible range
   * @return CPULimit with limit set
   * @throws IllegalArgumentException if limit does not conform to requirements
   */
  @throws[IllegalArgumentException]
  protected[core] def apply(cpuThreads: Double): CPULimit = {
    apply(cpuThreads.toFloat)
  }

  override protected[core] implicit val serdes = new RootJsonFormat[CPULimit] {
    def write(c: CPULimit) = JsNumber(c.threads)

    def read(value: JsValue) =
      Try {
        val JsNumber(cpuThreads) = value
        require(cpuThreads.isDecimalDouble || cpuThreads.isDecimalFloat, "CPU limit must be float number")
        CPULimit(cpuThreads.floatValue)
      } match {
        case Success(limit)                       => limit
        case Failure(e: IllegalArgumentException) => deserializationError(e.getMessage, e)
        case Failure(e: Throwable)                => deserializationError("CPU limit malformed", e)
      }
  }
}
