/**
 * Copyright 2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import static com.persistit.unit.ConcurrentUtil.createThread;
import static com.persistit.unit.ConcurrentUtil.startAndJoinAssertSuccess;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.exception.PersistitException;
import com.persistit.unit.ConcurrentUtil.ThrowingRunnable;

public class MVCCConcurrentTest extends MVCCTestBase {
    private final String KEY1 = "key1";

    @Test
    public void testReadWriteRemoveLongRecNoTrx() {
        final int NUM_OPS = 1000;
        final String LONG_STR = createString(ex1.getVolume().getPageSize() * 50);

        final Thread readThread = createThread("READ_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                final Exchange ex = getNewExchange();
                for (int i = 0; i < NUM_OPS; ++i) {
                    fetch(ex, KEY1, false);
                    final Value value = ex.getValue();
                    if (value.isDefined()) {
                        assertEquals("iteration i " + i, LONG_STR, value.getString());
                    }
                }
                _persistit.releaseExchange(ex);
            }
        });

        final Thread writeThread = createThread("WRITE_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                final Exchange ex = getNewExchange();
                for (int i = 0; i < NUM_OPS; ++i) {
                    store(ex, i, i);
                }
                _persistit.releaseExchange(ex);
            }
        });

        final Thread removeThread = createThread("REMOVE_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                final Exchange ex = getNewExchange();
                int j = 0;
                for (int i = 0; i < NUM_OPS; ++i, ++j) {
                    if (j == 0) {
                        store(ex, KEY1, LONG_STR);
                    } else if (j == 5) {
                        remove(ex, KEY1);
                        j = 0;
                    }
                }
            }
        });

        startAndJoinAssertSuccess(25000, readThread, writeThread, removeThread);
    }

    //
    // Test helpers
    //

    private Exchange getNewExchange() throws PersistitException {
        return _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
    }
}
