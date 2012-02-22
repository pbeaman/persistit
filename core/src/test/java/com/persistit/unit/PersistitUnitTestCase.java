/**
 * Copyright (C) 2011 Akiban Technologies Inc.
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

package com.persistit.unit;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Properties;

import com.persistit.exception.PersistitException;
import junit.framework.TestCase;

import com.persistit.Persistit;

public abstract class PersistitUnitTestCase extends TestCase {

    protected final static String RED_FOX = "The quick red fox jumped over the lazy brown dog.";

    protected Persistit _persistit = new Persistit();

    protected Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getProperties(cleanup);
    }

    @Override
    public void setUp() throws Exception {
        checkNoPersistitThreads();
        _persistit.initialize(getProperties(true));
    }

    @Override
    public void tearDown() throws Exception {
        final WeakReference<Persistit> ref = new WeakReference<Persistit>(_persistit);
        _persistit.close(false);
        _persistit = null;
        for (int count = 0; count < 100 && ref.get() != null; count++) {
            System.gc();
            Thread.sleep(100);
        }
        if (ref.get() != null) {
            System.out.println("Persistit has a leftover strong reference");
        }
        checkNoPersistitThreads();
    }

    public void runAllTests() throws Exception {

    }

    public void setPersistit(final Persistit persistit) {
        _persistit = persistit;
    }

    protected void initAndRunTest() throws Exception {
        setUp();
        try {
            runAllTests();
        } catch (final Throwable t) {
            t.printStackTrace();
        } finally {
            tearDown();
        }
    }

    private final static String[] PERSISTIT_THREAD_NAMES = { "CHECKPOINT_WRITER", "JOURNAL_COPIER", "JOURNAL_FLUSHER",
            "PAGE_WRITER", "TXN_UPDATE" };

    protected boolean checkNoPersistitThreads() {
        boolean alive = false;
        final Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
        for (final Thread t : map.keySet()) {
            String name = t.getName();
            for (final String p : PERSISTIT_THREAD_NAMES) {
                if (name.contains(p)) {
                    alive = true;
                    System.err.println("Thread " + t + " is still alive");
                }
            }
        }
        return alive;
    }

    protected void safeCrashAndRestoreProperties() throws PersistitException {
        Properties properties = _persistit.getProperties();
        _persistit.flush();
        _persistit.crash();
        _persistit = new Persistit();
        _persistit.initialize(properties);
    }
}
