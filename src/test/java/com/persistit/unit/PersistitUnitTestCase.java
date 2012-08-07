/**
 * Copyright © 2011-2012 Akiban Technologies, Inc.  All rights reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, version 3 (only) of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * This program may also be available under different license terms. For more
 * information, see www.akiban.com or contact licensing@akiban.com.
 */

package com.persistit.unit;

import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;

import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

public abstract class PersistitUnitTestCase {

    private final static long TEN_SECONDS = 10L * 1000L * 1000L * 1000L;

    protected final static String RED_FOX = "The quick red fox jumped over the lazy brown dog.";
    
    protected static String createString(int exactLength) {
        StringBuilder sb = new StringBuilder(exactLength);
        // Simple 0..9a..z string
        for (int i = 0; i < 36; ++i) {
            sb.append(Character.forDigit(i, 36));
        }
        final String numAndLetters = sb.toString();
        while (sb.length() < exactLength) {
            sb.append(numAndLetters);
        }
        return sb.toString().substring(0, exactLength);
    }

    protected Persistit _persistit = new Persistit();

    protected Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getProperties(cleanup);
    }

    @Before
    public void setUp() throws Exception {
        checkNoPersistitThreads();
        
        // Use warm-up capability on tests
        Properties prop = getProperties(true);
        prop.setProperty("bufferinventory", "/tmp/persistit_test_data");
        _persistit.initialize(prop);
    }

    @After
    public void tearDown() throws Exception {
        final WeakReference<Persistit> ref = new WeakReference<Persistit>(_persistit);
        _persistit.close(false);
        _persistit = null;

        if (!doesRefBecomeNull(ref)) {
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

    protected void crashWithoutFlushAndRestoreProperties() throws PersistitException {
        Properties properties = _persistit.getProperties();
        _persistit.crash();
        _persistit = new Persistit();
        _persistit.initialize(properties);
    }

    public static boolean doesRefBecomeNull(final WeakReference<?> ref) throws InterruptedException {
        long expires = System.nanoTime() + TEN_SECONDS;
        while (ref.get() != null && System.nanoTime() < expires) {
            System.gc();
            Thread.sleep(10);
        }
        return ref.get() == null;
    }
}
