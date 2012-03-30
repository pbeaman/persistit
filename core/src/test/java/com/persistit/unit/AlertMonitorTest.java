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

import com.persistit.AbstractAlertMonitor;
import com.persistit.AbstractAlertMonitor.AlertLevel;
import com.persistit.AbstractAlertMonitor.Event;
import com.persistit.AbstractAlertMonitor.History;
import com.persistit.logging.PersistitLevel;
import com.persistit.logging.PersistitLogger;

public class AlertMonitorTest extends PersistitUnitTestCase {

    private final static String CATEGORY = "XYABC";

    String _lastMessage;

    class MockPersistitLogger implements PersistitLogger {

        @Override
        public void log(PersistitLevel level, String message) {
            _lastMessage = message;
        }

        @Override
        public boolean isLoggable(PersistitLevel level) {
            return true;
        }

        @Override
        public void open() throws Exception {

        }

        @Override
        public void close() throws Exception {

        }

        @Override
        public void flush() {

        }

    }

    class MockAlertMonitor extends AbstractAlertMonitor {

        protected MockAlertMonitor() {
            super("Mock Alert Monitor");
        }
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _persistit.getLogBase().configure(new MockPersistitLogger());
    }

    public void testPostEvents() throws Exception {
        AbstractAlertMonitor monitor = new MockAlertMonitor();
        for (int index = 0; index < 100; index++) {
            monitor.post(new Event(_persistit.getLogBase().copyright, index), CATEGORY, AlertLevel.NORMAL);
        }
        History history = monitor.getHistory(CATEGORY);
        assertNotNull(history);
        assertEquals(100, history.getCount());
        assertEquals(0, history.getFirstEvent().getFirstArg());
        String s = monitor.getDetailedHistory(CATEGORY);
        assertTrue("Should contain first event", s.contains("   1:"));
        assertTrue("Should contain last event", s.contains(" 100:"));
        assertEquals("Should have 11 lines", 12, s.split("\n").length);
        s = history.toString();
        assertTrue("Should contain last event", s.contains(" 100:"));
        s = monitor.toString();
        assertTrue("Should contain last event", s.contains(" 100:"));
        assertTrue("Should contain last event", s.contains(CATEGORY));
        monitor.poll(true);
        assertNull("At AlertLevel.NORMAL there should not be a log message", _lastMessage);
        for (int index = 0; index < 100; index++) {
            monitor.post(new Event(_persistit.getLogBase().exception, new RuntimeException("Bogus " + index)),
                    CATEGORY, AlertLevel.ERROR);
        }
        monitor.poll(true);
        assertNotNull("At AlertLevel.ERROR there should be a log message", _lastMessage);
        assertTrue("Log message should contain RuntimeException", _lastMessage.contains("Bogus"));
        assertTrue("Log message should be recurring", _lastMessage.contains("similar"));
        _lastMessage = null;
        monitor.poll(true);
        assertNull("There must be only one report", _lastMessage);
        monitor.reset();
        assertEquals("History should have been cleared", "", monitor.getDetailedHistory(CATEGORY));
        assertEquals("History should have been cleared", "", monitor.toString());
        assertEquals("Level should be NORMAL", AlertLevel.NORMAL.toString(), monitor.getAlertLevel());
    }
}
