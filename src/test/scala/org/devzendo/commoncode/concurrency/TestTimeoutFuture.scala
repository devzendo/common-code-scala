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
import java.util.concurrent.atomic.AtomicBoolean

import org.devzendo.commoncode.logging.LoggingUnittest
import org.devzendo.commoncode.timeout.{DefaultTimeoutScheduler, TimeoutId, TimeoutScheduler}
import org.junit.{After, Assert, Before, Test}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.MustMatchers
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.AssertionsForJUnit
import org.scalatest.mock.MockitoSugar
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

object TestTimeoutFuture {
    val LOGGER: Logger = LoggerFactory.getLogger(TestTimeoutFuture.getClass)
}

class TestTimeoutFuture extends AssertionsForJUnit with MustMatchers with ScalaFutures with MockitoSugar with LoggingUnittest {

    import scala.concurrent.ExecutionContext.Implicits.global

    implicit val timeoutScheduler: TimeoutScheduler = new DefaultTimeoutScheduler

    @Before
    def startScheduler(): Unit = {
        timeoutScheduler.start()
    }

    @After
    def stopScheduler(): Unit = {
        timeoutScheduler.stop()
    }

    @Test(timeout = 4000L)
    def timeoutFutureCompletingInTimeSucceeds(): Unit = {
        val succeeded = new AtomicBoolean(false)

        val future: Future[Boolean] = TimeoutFuture(1000L, {
            val bool = true
            succeeded.set(bool)
            Success(bool)
        })

        ThreadUtils.waitNoInterruption(1500L) // wait for the timeout period to finish

        Assert.assertTrue(succeeded.get())
        future.value must be('defined)
        future.value.get match {
            case Success(x) =>
                x mustBe true
                // Can't test here that the timeout scheduler has cancelled the timeout
            case Failure(_) =>
                fail ("Should not have timed out")
        }
    }

    @Test(timeout = 4000L)
    def timeoutFutureCompletingWithThrowInTimeFails(): Unit = {
        val future: Future[Boolean] = TimeoutFuture(1000L, {
            throw new IllegalStateException("Flux capacitor overload") // not exactly FP, but deal with it!
        })

        ThreadUtils.waitNoInterruption(1500L) // wait for the timeout period to finish

        future.value must be('defined)
        future.value.get match {
            case Success(x) =>
                fail ("Should not have succeeded")
            // Can't test here that the timeout scheduler has cancelled the timeout
            case Failure(f) =>
                f mustBe a [IllegalStateException]
                f.getMessage must be("Flux capacitor overload")
        }
    }

    @Test(timeout = 4000L)
    def timeoutFutureCompletingWithFailureInTimeFails(): Unit = {
        val future: Future[Boolean] = TimeoutFuture(1000L, {
            Failure(new IllegalStateException("Flux capacitor overload"))
        })

        ThreadUtils.waitNoInterruption(1500L) // wait for the timeout period to finish

        future.value must be('defined)
        future.value.get match {
            case Success(x) =>
                fail ("Should not have succeeded")
            // Can't test here that the timeout scheduler has cancelled the timeout
            case Failure(f) =>
                f mustBe a [IllegalStateException]
                f.getMessage must be("Flux capacitor overload")
        }
    }

    @Test(timeout = 4000L)
    def timeoutFutureCompletingCancelsTimeout(): Unit = {
        val mockTimeoutScheduler = mock[TimeoutScheduler]
        val timeoutId = new TimeoutId(69L)
        when(mockTimeoutScheduler.schedule(ArgumentMatchers.eq(1000L), ArgumentMatchers.any[Runnable])).thenReturn(timeoutId)

        TimeoutFuture(1000L, { Success(true) })(mockTimeoutScheduler, Implicits.global)

        ThreadUtils.waitNoInterruption(1500L) // wait for the timeout period to finish

        verify(mockTimeoutScheduler, times(1)).cancel(timeoutId)
    }

    @Test(timeout = 4000L)
    def timeoutFutureCompletingAfterTimeoutFailsWithFutureHoldingFailureTimeoutException(): Unit = {
        val succeeded = new AtomicBoolean(false)

        val future: Future[Boolean] = TimeoutFuture(1000L, {
            ThreadUtils.waitNoInterruption(2000L)
            val bool = true
            succeeded.set(bool)
            Success(bool)
        })

        ThreadUtils.waitNoInterruption(500L)

        // Initially, nothing happens...
        Assert.assertFalse(succeeded.get())
        future.value must not be 'defined

        ThreadUtils.waitNoInterruption(1000L)

        // should have finished with timeout by now
        Assert.assertFalse(succeeded.get())
        future.value must be('defined)
        future.value.get match {
            case Success(_) =>
                fail ("Should not get a success from a timeout")
            case Failure(x) =>
                x mustBe a [TimeoutException]
                x.getMessage must include("Timed out after 1000ms")
                // It's implied that the timeout scheduler must have executed the timeout
        }
    }

    @Test(timeout = 4000L)
    def timeoutFutureCompletingAfterTimeoutTimeCallsOnTimeoutBody(): Unit = {
        val onTimeoutBodyCalled = new AtomicBoolean(false)

        val future: Future[Boolean] = TimeoutFuture(1000L, {
            ThreadUtils.waitNoInterruption(2000L)
            Success(true)
        }, {
            onTimeoutBodyCalled.set(true)
        })

        ThreadUtils.waitNoInterruption(1500L)

        // should have finished with timeout by now
        future.value must be('defined)
        future.value.get match {
            case Success(_) =>
                fail ("Should not get a success from a timeout")
            case Failure(_) =>
                onTimeoutBodyCalled.get() must be(true)
            // It's implied that the timeout scheduler must have executed the timeout
        }
    }

    @Test(timeout = 4000L)
    def timeoutFutureCompletingAfterTimeoutCallsOnTimeoutBodyAcceptingPromiseToFailWithCustomException(): Unit = {

        val onTimeoutBodyCalled: (Promise[Boolean] => Unit) = promise => {
            TestTimeoutFuture.LOGGER.info("In timeout body with promise, failing in custom manner")
            promise.failure(new ArithmeticException("My custom exception text"))
        }

        val future: Future[Boolean] = TimeoutFuture(1000L, {
            ThreadUtils.waitNoInterruption(2000L)
            Success(true)
        }, onTimeoutBodyCalled)

        ThreadUtils.waitNoInterruption(1500L)

        // should have finished with timeout by now
        future.value must be('defined)
        future.value.get match {
            case Success(_) =>
                fail ("Should not get a success from a timeout")
            case Failure(x) =>
                x mustBe a [ArithmeticException]
                x.getMessage must include("My custom exception text")
            // It's implied that the timeout scheduler must have executed the timeout
        }
    }

    @Test(timeout = 4000L)
    def timeoutFutureCompletingAfterTimeoutCallsOnTimeoutBodyAcceptingPromiseThatDoesNotFailWithCustomExceptionButTimeoutExceptionStillSetAsFailure(): Unit = {

        val onTimeoutBodyCalled: (Promise[Boolean] => Unit) = _ => {
            TestTimeoutFuture.LOGGER.info("In timeout body with promise, ignoring opportunity to fail in custom manner")
        }

        val future: Future[Boolean] = TimeoutFuture(1000L, {
            ThreadUtils.waitNoInterruption(2000L)
            Success(true)
        }, onTimeoutBodyCalled)

        ThreadUtils.waitNoInterruption(1500L)

        // should have finished with timeout by now
        future.value must be('defined)
        future.value.get match {
            case Success(_) =>
                fail ("Should not get a success from a timeout")
            case Failure(x) =>
                x mustBe a [TimeoutException]
                x.getMessage must include("Timed out after 1000ms")
            // It's implied that the timeout scheduler must have executed the timeout
        }
    }
}

