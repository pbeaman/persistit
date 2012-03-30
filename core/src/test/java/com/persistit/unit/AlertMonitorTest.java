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
