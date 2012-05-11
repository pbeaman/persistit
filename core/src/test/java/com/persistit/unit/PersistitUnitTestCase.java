/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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

    protected Persistit _persistit = new Persistit();

    protected Properties getProperties(final boolean cleanup) {
        return UnitTestProperties.getProperties(cleanup);
    }

    @Before
    public void setUp() throws Exception {
        checkNoPersistitThreads();
        _persistit.initialize(getProperties(true));
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
