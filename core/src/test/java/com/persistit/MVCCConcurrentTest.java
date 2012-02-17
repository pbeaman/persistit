/**
 * Copyright (C) 2012 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.persistit;

import com.persistit.exception.PersistitException;

import java.util.Map;
import java.util.TreeMap;

public class MVCCConcurrentTest extends MVCCTestBase {
    private final String KEY1 = "key1";

    private final Map<String, Throwable> uncaughtExceptions = new TreeMap<String, Throwable>();
    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            synchronized(uncaughtExceptions) {
                uncaughtExceptions.put(t.getName(), e);
            }
            Thread.getDefaultUncaughtExceptionHandler().uncaughtException(t, e);
        }
    };

    @Override
    public void setUp() throws Exception {
        super.setUp();
        uncaughtExceptions.clear();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        assertEquals("Uncaught exceptions", "{}", uncaughtExceptions.toString());
    }


    public void testReadWriteRemoveLongRecNoTrx() {
        final int NUM_OPS = 1000;
        final String LONG_STR = createString(ex1.getVolume().getPageSize() * 50);

        Thread readThread = createThread(new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                Exchange ex = getNewExchange();
                for(int i = 0; i < NUM_OPS; ++i) {
                    fetch(ex, KEY1, false);
                    Value value = ex.getValue();
                    if (value.isDefined()) {
                        assertEquals("iteration i " + i, LONG_STR, value.getString());
                    }
                }
                _persistit.releaseExchange(ex);
            }
        }, "READ_THREAD");

        Thread writeThread = createThread(new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                Exchange ex = getNewExchange();
                for(int i = 0; i < NUM_OPS; ++i) {
                    store(ex, i, i);
                }
                _persistit.releaseExchange(ex);
            }
        }, "WRITE_THREAD");

        Thread removeThread = createThread(new ThrowingRunnable() {
            @Override
            public void run() throws Exception {
                Exchange ex = getNewExchange();
                int j = 0;
                for(int i = 0; i < NUM_OPS; ++i, ++j) {
                    if (j == 0) {
                        store(ex, KEY1, LONG_STR);
                    } else if (j == 5) {
                        remove(ex, KEY1);
                        j = 0;
                    }
                }
            }
        }, "REMOVE_THREAD");

        startAndJoinAll(readThread, writeThread, removeThread);
    }


    //
    // Test helpers
    //


    private static interface ThrowingRunnable {
        public void run() throws Exception;
    }

    private Thread createThread(final ThrowingRunnable runnable, final String name) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    runnable.run();
                } catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }, name);
        t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return t;
    }

    private void startAndJoinAll(Thread... threads) {
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            for (;;) {
                try {
                    t.join();
                    break;
                } catch(InterruptedException e) {
                    System.err.println("Interrupted but continuing thread: " + t.getName());
                }
            }
        }
    }

    private Exchange getNewExchange() throws PersistitException {
        return _persistit.getExchange(TEST_VOLUME_NAME, TEST_TREE_NAME, true);
    }
}