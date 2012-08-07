/**
 * Copyright © 2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit;

import static com.persistit.unit.ConcurrentUtil.createThread;
import static com.persistit.unit.ConcurrentUtil.startAndJoinAssertSuccess;
import static com.persistit.unit.ConcurrentUtil.ThrowingRunnable;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.persistit.exception.PersistitException;

public class MVCCConcurrentTest extends MVCCTestBase {
    private final String KEY1 = "key1";

    @Test
    public void testReadWriteRemoveLongRecNoTrx() {
        final int NUM_OPS = 1000;
        final String LONG_STR = createString(ex1.getVolume().getPageSize() * 50);

        Thread readThread = createThread("READ_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                Exchange ex = getNewExchange();
                for (int i = 0; i < NUM_OPS; ++i) {
                    fetch(ex, KEY1, false);
                    Value value = ex.getValue();
                    if (value.isDefined()) {
                        assertEquals("iteration i " + i, LONG_STR, value.getString());
                    }
                }
                _persistit.releaseExchange(ex);
            }
        });

        Thread writeThread = createThread("WRITE_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                Exchange ex = getNewExchange();
                for (int i = 0; i < NUM_OPS; ++i) {
                    store(ex, i, i);
                }
                _persistit.releaseExchange(ex);
            }
        });

        Thread removeThread = createThread("REMOVE_THREAD", new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                Exchange ex = getNewExchange();
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

        startAndJoinAssertSuccess(5000, readThread, writeThread, removeThread);
    }

    //
    // Test helpers
    //

    private Exchange getNewExchange() throws PersistitException {
        return _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
    }
}

