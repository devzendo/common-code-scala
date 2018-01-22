/*
 * Copyright (C) 2008-2017 Matt Gumbley, DevZendo.org http://devzendo.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.devzendo.commoncode.concurrency

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong

import org.devzendo.commoncode.timeout.TimeoutScheduler
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future, Promise}

object TimeoutFuture {
    val LOGGER: Logger = LoggerFactory.getLogger(TimeoutFuture.getClass)
    val count = new AtomicLong(0)

    /**
      * Create a Future[T] that times out after the requested number of milliseconds, failing the Future with
      * Failure[TimeoutException] in that case, then calling the onTimeoutBody to permit client code to tidy up.
      * If execution of the executionBody completes before timeoutMs milliseconds have elapsed, its value is used to
      * successfully complete the Future, and the timeout is cancelled, with the onTimeoutBody not called.
      * @param timeoutMs the duration in milliseconds that the executionBody is given in which to execute.
      * @param executionBody the body of code that returns a T, that will be used to successfully complete the Future
      *                      if the timeout does not occur.
      * @param onTimeoutBody an optional body of code that is executed if the executionBody takes longer than timeoutMs
      *                      to complete.
      * @param timeoutScheduler the implicit TimeoutScheduler used to schedule and cancel the timeout.
      * @param executor the implicit ExecutionContext used to run the executionBody.
      * @tparam T the type of the Future
      * @return the Future[T] that can be used asynchronously.
      */
    def apply[T](timeoutMs: Long, executionBody: => T, onTimeoutBody: => Unit = {})(implicit timeoutScheduler: TimeoutScheduler, executor: ExecutionContext): Future[T] = {
        val thisCount = count.getAndIncrement()
        val messagePrefix = "TimeoutFuture #" + thisCount + ": "

        val promise = Promise[T]()

        val timeoutId = timeoutScheduler.schedule(timeoutMs, new Runnable {
            override def run(): Unit = {
                val str = messagePrefix + "Timed out after " + timeoutMs + "ms"
                LOGGER.debug(str)

                // Potential for race condition if executionBody has just completed with success: this setting of a
                // failure will throw.
                try {
                    promise.failure(new TimeoutException(str))
                } catch {
                    case (ise: IllegalStateException) =>
                        LOGGER.warn(messagePrefix + "Could not set Failure[TimeoutException] since Promise already completed") // otherwise, this can be ignored.
                }

                try {
                    LOGGER.debug(messagePrefix + "Calling timeout handler")
                    onTimeoutBody
                    LOGGER.debug(messagePrefix + "Called timeout handler")
                } catch {
                    case (e: Exception) =>
                        LOGGER.warn(messagePrefix + "Timeout handler threw " + e.getClass.getSimpleName + ": " + e.getMessage, e)
                }
            }
        })

        // Start executing the execution body....
        LOGGER.debug(messagePrefix + "Scheduling execution with timeout id " + timeoutId)
        Future({
            // Potential for race condition if executionBody has just completed with timeout (failure): this setting of
            // a success will throw.
            try {
                LOGGER.debug(messagePrefix + "Starting execution with timeout id " + timeoutId)
                promise.success(executionBody)
                LOGGER.debug(messagePrefix + "Finished successful execution with timeout id " + timeoutId)
            } catch {
                case (ise: IllegalStateException) =>
                    LOGGER.warn(messagePrefix + "Could not set Success since Promise already completed with Failure[TimeoutException]") // otherwise, this can be ignored.
            }
            LOGGER.debug(messagePrefix + "Cancelling Timeout " + timeoutId)
            timeoutScheduler.cancel(timeoutId)
        })

        promise.future
    }
}

