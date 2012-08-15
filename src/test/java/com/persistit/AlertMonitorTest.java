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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import javax.management.MBeanServer;
import javax.management.Notification;
import javax.management.NotificationListener;

import org.junit.Test;

import com.persistit.AlertMonitor.AlertLevel;
import com.persistit.AlertMonitor.Event;
import com.persistit.AlertMonitor.History;
import com.persistit.logging.PersistitLevel;
import com.persistit.logging.PersistitLogMessage.LogItem;
import com.persistit.logging.PersistitLogger;

public class AlertMonitorTest extends PersistitUnitTestCase {

    private final static String CATEGORY = "XYABC";

    String _lastMessage;
    Notification _notification;
    int _added;
    int _removed;

    class MockPersistitLogger implements PersistitLogger {

        @Override
        public void log(final PersistitLevel level, final String message) {
            _lastMessage = message;
        }

        @Override
        public boolean isLoggable(final PersistitLevel level) {
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

    class AggregatingEvent extends Event {

        AggregatingEvent(final AlertLevel level, final LogItem logItem, final Object... args) {
            super(level, logItem, args);
        }

        @Override
        protected void added(final History h) {
            _added++;
        }

        @Override
        protected void removed(final History h) {
            _removed++;
        }

    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        _persistit.getLogBase().configure(new MockPersistitLogger());
    }

    @Test
    public void testPostEvents() throws Exception {
        final AlertMonitor monitor = _persistit.getAlertMonitor();
        for (int index = 0; index < 100; index++) {
            monitor.post(new Event(AlertLevel.NORMAL, _persistit.getLogBase().copyright, index), CATEGORY);
        }
        final History history = monitor.getHistory(CATEGORY);
        assertNotNull(history);
        assertEquals("Event count should be 100", 100, history.getCount());
        assertEquals("First event argument should be 0", 0, history.getFirstEvent().getFirstArg());
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
            monitor.post(new Event(AlertLevel.ERROR, _persistit.getLogBase().exception, new RuntimeException("Bogus "
                    + index)), CATEGORY);
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

    @Test
    public void testChangeHistoryLength() throws Exception {
        final AlertMonitor monitor = _persistit.getAlertMonitor();
        for (int index = 0; index < 100; index++) {
            monitor.post(new AggregatingEvent(AlertLevel.NORMAL, _persistit.getLogBase().copyright, index), CATEGORY);
        }
        final History history = monitor.getHistory(CATEGORY);
        assertEquals("History should have 10 events", 10, history.getEventList().size());
        monitor.setHistoryLength(5);
        assertEquals("History should now have 5 events", 5, history.getEventList().size());
        monitor.post(new AggregatingEvent(AlertLevel.NORMAL, _persistit.getLogBase().copyright, 101), CATEGORY);
        assertEquals("History should still have 5 events", 5, history.getEventList().size());
        assertEquals("Total of 101 events added", 101, _added);
        assertEquals("Total of 101-5 events removed", 101 - 5, _removed);
    }

    @Test
    public void testNotifications() throws Exception {
        final AlertMonitor monitor = _persistit.getAlertMonitor();
        final MBeanServer server = java.lang.management.ManagementFactory.getPlatformMBeanServer();
        server.addNotificationListener(_persistit.getAlertMonitor().getObjectName(), new NotificationListener() {
            @Override
            public void handleNotification(final Notification notification, final Object handback) {
                ((AlertMonitorTest) handback)._notification = notification;
            }
        }, null, this);
        monitor.post(new Event(AlertLevel.ERROR, _persistit.getLogBase().copyright, 2012), CATEGORY);
        Thread.sleep(1000);
        assertNotNull(_notification);
    }
}
