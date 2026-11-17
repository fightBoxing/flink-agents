/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.agents.runtime.async;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies retry backoff yields the mailbox thread on JDK 21. The JDK 11 executor cannot yield, so
 * the test is skipped unless the multi-release continuation implementation is on the classpath.
 */
class ContinuationSuspendTest {

    @Test
    void suspendYieldsBeforeDelayElapses() throws Exception {
        Assumptions.assumeTrue(ContinuationActionExecutor.isContinuationSupported());

        ContinuationActionExecutor executor = new ContinuationActionExecutor(1);
        ContinuationContext context = new ContinuationContext();
        AtomicBoolean finished = new AtomicBoolean();
        try {
            long startNanos = System.nanoTime();
            boolean done =
                    executor.executeAction(
                            context,
                            () -> {
                                try {
                                    executor.suspend(context, 2_000L);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                                finished.set(true);
                            });
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;

            assertFalse(done);
            assertFalse(finished.get());
            assertTrue(elapsedMs < 500L, "suspend held the caller for " + elapsedMs + " ms");

            long deadlineNanos = System.nanoTime() + 5_000_000_000L;
            while (!done && System.nanoTime() < deadlineNanos) {
                Thread.sleep(20L);
                done = executor.executeAction(context, () -> {});
            }
            assertTrue(finished.get());
            assertTrue(done);
        } finally {
            executor.close();
        }
    }
}
