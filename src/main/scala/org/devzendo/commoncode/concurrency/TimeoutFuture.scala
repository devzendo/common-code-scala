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
import scala.util.{Failure, Try}

object TimeoutFuture {
    val LOGGER: Logger = LoggerFactory.getLogger(TimeoutFuture.getClass)
    val count = new AtomicLong(0)

    /**
      * Create a Future[T] that times out after the requested number of milliseconds, calling the onTimeoutBody to
      * permit client code to tidy up, then failing the Future with Failure[TimeoutException] in that case.
      * If execution of the executionBody completes before timeoutMs milliseconds have elapsed, its value is used to
      * successfully complete the Future, and the timeout is cancelled, with the onTimeoutBody not called.
      *
      * @param timeoutMs        the duration in milliseconds that the executionBody is given in which to execute.
      * @param executionBodyTry the body of code that returns a Try[T], that will be used to successfully complete the
      *                         Future if the timeout does not occur.
      * @param onTimeoutBody    a body of code that is executed if the executionBody takes longer than timeoutMs
      *                         to complete.
      * @param timeoutScheduler the implicit TimeoutScheduler used to schedule and cancel the timeout.
      * @param executor         the implicit ExecutionContext used to run the executionBody.
      * @tparam T the type of the Future
      * @return the Future[T] that can be used asynchronously.
      */
    def apply[T](timeoutMs: Long, executionBodyTry: => Try[T], onTimeoutBody: => Unit)(implicit timeoutScheduler: TimeoutScheduler, executor: ExecutionContext): Future[T] = {

        def onTimeoutBodyDoesntNeedPromise[T]: (Promise[T] => Unit) = promise => {
            onTimeoutBody
        }

        innerApply[T](timeoutMs, executionBodyTry, onTimeoutBodyDoesntNeedPromise)
    }

    /**
      * Create a Future[T] that times out after the requested number of milliseconds, failing the Future with
      * Failure[TimeoutException] in that case.
      * If execution of the executionBody completes before timeoutMs milliseconds have elapsed, its value is used to
      * successfully complete the Future, and the timeout is cancelled.
      *
      * @param timeoutMs        the duration in milliseconds that the executionBody is given in which to execute.
      * @param executionBodyTry the body of code that returns a Try[T], that will be used to successfully complete the
      *                         Future if the timeout does not occur.
      * @param timeoutScheduler the implicit TimeoutScheduler used to schedule and cancel the timeout.
      * @param executor         the implicit ExecutionContext used to run the executionBody.
      * @tparam T the type of the Future
      * @return the Future[T] that can be used asynchronously.
      */
    def apply[T](timeoutMs: Long, executionBodyTry: => Try[T])(implicit timeoutScheduler: TimeoutScheduler, executor: ExecutionContext): Future[T] = {
        innerApply(timeoutMs, executionBodyTry, doNothingOnTimeoutBody)
    }

    /**
      * Create a Future[T] that times out after the requested number of milliseconds, then calling the onTimeoutBody to
      * permit client code to tidy up and set any specific Failure in the Future (via the passed Promise). If the
      * onTimeoutBody does not set a Failure in the Future, a default Failure[TimeoutException] will be set.
      * If execution of the executionBody completes before timeoutMs milliseconds have elapsed, its value is used to
      * successfully complete the Future, and the timeout is cancelled, with the onTimeoutBody not called.
      *
      * @param timeoutMs        the duration in milliseconds that the executionBody is given in which to execute.
      * @param executionBodyTry the body of code that returns a Try[T], that will be used to successfully complete the
      *                         Future if the timeout does not occur.
      * @param onTimeoutBody    an optional body of code that is executed if the executionBody takes longer than
      *                         timeoutMs to complete. This code is given the Promise against which a custom Failure
      *                         can be set.
      * @param timeoutScheduler the implicit TimeoutScheduler used to schedule and cancel the timeout.
      * @param executor         the implicit ExecutionContext used to run the executionBody.
      * @tparam T the type of the Future
      * @return the Future[T] that can be used asynchronously.
      */
    def apply[T](timeoutMs: Long, executionBodyTry: => Try[T], onTimeoutBody: Promise[T] => Unit = doNothingOnTimeoutBody)(implicit timeoutScheduler: TimeoutScheduler, executor: ExecutionContext): Future[T] = {
        innerApply(timeoutMs, executionBodyTry, onTimeoutBody)
    }

    private def doNothingOnTimeoutBody[T]: (Promise[T] => Unit) = promise => {}

    private def innerApply[T](timeoutMs: Long, executionBodyTry: => Try[T], onTimeoutBody: Promise[T] => Unit)(implicit timeoutScheduler: TimeoutScheduler, executor: ExecutionContext) = {
        val thisCount = count.getAndIncrement()
        val messagePrefix = "TimeoutFuture #" + thisCount + ": "

        val promise = Promise[T]()

        val timeoutId = timeoutScheduler.schedule(timeoutMs, new Runnable {
            override def run(): Unit = {
                val str = messagePrefix + "Timed out after " + timeoutMs + "ms"
                LOGGER.debug(str)

                try {
                    LOGGER.debug(messagePrefix + "Calling timeout handler")
                    onTimeoutBody(promise)
                    LOGGER.debug(messagePrefix + "Called timeout handler")
                } catch {
                    case (e: Exception) =>
                        LOGGER.warn(messagePrefix + "Timeout handler threw " + e.getClass.getSimpleName + ": " + e.getMessage, e)
                }

                // Potential for race condition if executionBody has just completed with success: this setting of a
                // failure will throw.
                if (!promise.tryComplete(Failure(new TimeoutException(str)))) {
                    LOGGER.debug(messagePrefix + "Could not set Failure[TimeoutException] since Promise already completed") // otherwise, this can be ignored.
                    // Having this any higher than debug will cause noise
                }
            }
        })

        // Start executing the execution body....
        LOGGER.debug(messagePrefix + "Scheduling execution with timeout id " + timeoutId)
        Future({
            LOGGER.debug(messagePrefix + "Starting execution with timeout id " + timeoutId)
            val tryBody = {
                try {
                    executionBodyTry
                } catch {
                    case (e: Exception) =>
                        Failure(e)
                }
            }
            if (!promise.tryComplete(tryBody)) {
                LOGGER.debug(messagePrefix + "Could not set execution result since Promise already completed with " + promise.future.value) // otherwise, this can be ignored.
                // Having this any higher than debug will cause noise
            }
            LOGGER.debug(messagePrefix + "Finished execution with timeout id " + timeoutId)

            LOGGER.debug(messagePrefix + "Cancelling Timeout " + timeoutId)
            timeoutScheduler.cancel(timeoutId)
        })

        promise.future
    }
}

