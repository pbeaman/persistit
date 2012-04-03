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

package com.persistit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import javax.management.MBeanNotificationInfo;
import javax.management.Notification;
import javax.management.NotificationBroadcasterSupport;
import javax.management.ObjectName;

import com.persistit.logging.LogBase;
import com.persistit.logging.PersistitLogMessage.LogItem;
import com.persistit.mxbeans.AlertMonitorMXBean;
import com.persistit.util.Util;

/**
 * Manage the process of accumulating and logging abnormal events such as
 * IOExceptions and measurements outside of expected thresholds. Concrete
 * AbstractAlertMonitor implementations are set up and registered as MXBeans
 * during Persistit initialization, and their behavior can be modified through
 * the MXBean interface.
 * 
 * @author peter
 * 
 */
public class AlertMonitor extends NotificationBroadcasterSupport implements AlertMonitorMXBean {

    private final static int DEFAULT_HISTORY_LENGTH = 10;
    private final static int MINIMUM_HISTORY_LENGTH = 1;
    private final static int MAXIMUM_HISTORY_LENGTH = 1000;

    private final static long DEFAULT_WARN_INTERVAL = 600000;
    private final static long MINIMUM_WARN_INTERVAL = 1000;
    private final static long MAXIMUM_WARN_INTERVAL = 86400000;

    private final static long DEFAULT_ERROR_INTERVAL = 15000;
    private final static long MINIMUM_ERROR_INTERVAL = 1000;
    private final static long MAXIMUM_ERROR_INTERVAL = 86400000;

    public enum AlertLevel {
        /*
         * Normal state
         */
        NORMAL,
        /*
         * Warning - system is running but could be experiencing trouble that
         * could lead to an error. Example: too many journal files, disk is
         * filling up, pruning is falling behind etc.
         */
        WARN,
        /*
         * Error - system is generating errors and failing. Example: disk is
         * full.
         */
        ERROR,
    }

    protected final static String EVENT_FORMAT = "event %,5d: %s";
    protected final static String AGGREGATION_FORMAT = "Minimum=%,d Maximum=%,d Total=%,d";
    protected final static String EXTRA_FORMAT = "Extra=%s";

    public class History {
        private AlertLevel _level = AlertLevel.NORMAL;
        private List<Event> _eventList = new ArrayList<Event>();
        private volatile long _firstEventTime = Long.MAX_VALUE;
        private volatile long _lastWarnLogTime = Long.MIN_VALUE;
        private volatile long _lastErrorLogTime = Long.MIN_VALUE;
        private volatile int _reportedCount;
        private volatile Event _firstEvent;
        private volatile int _count;
        private volatile long _minimum;
        private volatile long _maximum;
        private volatile long _total;
        private volatile Object _extra;

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            synchronized (AlertMonitor.this) {
                final Event event = getLastEvent();
                if (_count > 0) {
                    sb.append(String.format(EVENT_FORMAT, _count, event == null ? "missing" : format(event)));
                }
            }
            return sb.toString();
        }

        public String getDetailedHistory() {
            final StringBuilder sb = new StringBuilder();
            synchronized (AlertMonitor.this) {
                int size = _eventList.size();
                if (_count > 0) {
                    sb.append(String.format(EVENT_FORMAT, 1, format(_firstEvent)));
                    for (int index = _count > size ? 0 : 1; index < size; index++) {
                        if (sb.length() > 0) {
                            sb.append(Util.NEW_LINE);
                        }
                        sb.append(String.format(EVENT_FORMAT, _count - size + index + 1, format(_eventList.get(index))));
                    }
                    if (_minimum != 0 || _maximum != 0 || _total != 0) {
                        if (sb.length() > 0) {
                            sb.append(Util.NEW_LINE);
                        }
                        sb.append(String.format(AGGREGATION_FORMAT, _minimum, _maximum, _total));
                    }
                    if (_extra != null) {
                        if (sb.length() > 0) {
                            sb.append(Util.NEW_LINE);
                        }
                        sb.append(String.format(EXTRA_FORMAT, _extra));
                    }
                }
            }
            return sb.toString();
        }

        /**
         * @return the current alert level of this monitor
         */
        public AlertLevel getLevel() {
            return _level;
        }

        /**
         * @return the minimum
         */
        public long getMinimum() {
            return _minimum;
        }

        /**
         * @param minimum
         *            the minimum to set
         */
        public void setMinimum(long minimum) {
            _minimum = minimum;
        }

        /**
         * @return the maximum
         */
        public long getMaximum() {
            return _maximum;
        }

        /**
         * @param maximum
         *            the maximum to set
         */
        public void setMaximum(long maximum) {
            _maximum = maximum;
        }

        /**
         * @return the total
         */
        public long getTotal() {
            return _total;
        }

        /**
         * @param total
         *            the total to set
         */
        public void setTotal(long total) {
            _total = total;
        }

        /**
         * @return the extra
         */
        public Object getExtra() {
            return _extra;
        }

        /**
         * @param extra
         *            the extra to set
         */
        public void setExtra(Object extra) {
            _extra = extra;
        }

        /**
         * @return time of the first event
         */
        public long getFirstEventTime() {
            return _firstEventTime;
        }

        /**
         * @return time of the last event
         */
        public long getLastEventTime() {
            synchronized (AlertMonitor.this) {
                if (_eventList.size() > 0) {
                    return _eventList.get(_eventList.size() - 1)._time;
                } else {
                    return Long.MIN_VALUE;
                }
            }
        }

        /**
         * @return interval between first and last recorded event, in seconds
         */
        public long getDuration() {
            final long latest = getLastEventTime();
            if (latest == Long.MIN_VALUE) {
                return 0;
            }
            return (latest - _firstEventTime) / 1000;
        }

        /**
         * @return the recent history
         */
        public List<Event> getEventList() {
            synchronized (AlertMonitor.this) {
                return new ArrayList<Event>(_eventList);
            }
        }

        /**
         * @return The first event added to this history, or <code>null</code>
         *         if there have been no events.
         */
        public Event getFirstEvent() {
            return _firstEvent;
        }

        /**
         * @return The last event added to this history, or <code>null</code> if
         *         there have been no events.
         */
        public Event getLastEvent() {
            synchronized (AlertMonitor.this) {
                if (_eventList.isEmpty()) {
                    return null;
                } else {
                    return _eventList.get(_eventList.size() - 1);
                }
            }
        }

        /**
         * @return the count of events posted to this history
         */
        public int getCount() {
            return _count;
        }

        /**
         * Emit a log message to signify an ongoing condition. This method is
         * called periodically. It keeps track of when the last message was
         * added to the log and writes a new recurring message only as
         * frequently as allowed by
         * {@link AlertMonitor#getErrorLogTimeInterval()} or
         * {@link AlertMonitor#getWarnLogTimeInterval()}.
         */
        public void poll(final long now, final boolean force) {
            int count = getCount();
            if (count > _reportedCount) {
                switch (_level) {
                case ERROR:
                    if (force || now > _lastErrorLogTime + _errorLogTimeInterval) {
                        _lastErrorLogTime = now;
                        log(this);
                        sendNotification(this);
                        _reportedCount = count;
                    }
                    break;
                case WARN:
                    if (force || now > _lastWarnLogTime + _warnLogTimeInterval) {
                        _lastWarnLogTime = now;
                        log(this);
                        sendNotification(this);
                        _reportedCount = count;
                    }
                    break;

                default:
                    // Ignore the NORMAL case
                }
            }
        }

        private void addEvent(final Event event, final AlertLevel level) {
            int size = Math.max(_historyLength, 1);
            while (_eventList.size() >= size) {
                _eventList.remove(0);
            }
            _eventList.add(event);
            _count++;
            if (event.getTime() < _firstEventTime) {
                _firstEventTime = event.getTime();
                _firstEvent = event;
            }
            _level = translateLevel(event, level, this);

        }
    }

    /**
     * Holder for event data including the event arguments and the time the
     * event was posted.
     * 
     */
    public static class Event {
        private final LogItem _logItem;
        private final Object[] _args;
        private final long _time;

        public Event(LogItem logItem, Object... args) {
            this(System.currentTimeMillis(), logItem, args);
        }

        public Event(long time, LogItem logItem, Object... args) {
            _logItem = logItem;
            _args = args;
            _time = time;
        }

        public LogItem getLogItem() {
            return _logItem;
        }

        public long getTime() {
            return _time;
        }

        public Object[] getArgs() {
            return _args;
        }

        public Object getFirstArg() {
            return _args.length > 0 ? _args[0] : null;
        }

        @Override
        public String toString() {
            return Util.date(_time) + " " + _logItem.logMessage(_args);
        }
    }

    final static String NOTIFICATION_TYPE = "com.persistit.AlertMonitor";

    private final Map<String, History> _historyMap = new TreeMap<String, History>();

    private volatile long _warnLogTimeInterval = DEFAULT_WARN_INTERVAL;
    private volatile long _errorLogTimeInterval = DEFAULT_ERROR_INTERVAL;
    private volatile int _historyLength = DEFAULT_HISTORY_LENGTH;

    private AtomicLong _notificationSequence = new AtomicLong();
    private volatile ObjectName _objectName;

    public AlertMonitor() {
        super(Executors.newCachedThreadPool());
    }
    
    void setObjectName(final ObjectName on) {
        _objectName = on;
    }
    
    ObjectName getObjectName() {
        return _objectName;
    }

    /**
     * Post an event. The event can describe an Exception, a String, or other
     * kind of object understood by the
     * {@link #translateCategory(Object, AlertLevel)} method. Does nothing if
     * <code>level</code> is {@link AlertLevel#NORMAL}.
     * 
     * @param event
     *            A Event object describing what happened
     * @param level
     *            Indicates whether this event is a warning or an error.
     * @param time
     *            at which event occurred
     */
    public synchronized final void post(Event event, final String category, AlertLevel level) {
        final String translatedCategory = translateCategory(event, category);
        History history = _historyMap.get(translatedCategory);
        if (history == null) {
            history = new History();
            _historyMap.put(translatedCategory, history);
        }
        aggregate(event, history, level);

        history.addEvent(event, level);
        history.poll(event.getTime(), false);

    }

    /**
     * Perform extended aggregation of the history. For example, a subclass may
     * extend this method the maintain the minimum, maximum and total fields of
     * the History object. By default this method does nothing.
     * 
     * @param event
     * @param history
     * @param level
     */
    protected void aggregate(Event event, History history, AlertLevel level) {
        // Default: do nothing
    }


    /**
     * Restore this alert monitor to level {@link AlertLevel#NORMAL} with no
     * history. The logging time intervals and history length are not changed.
     */
    @Override
    public synchronized void reset() {
        _historyMap.clear();
    }

    /**
     * @return the interval in milliseconds between successive log entries for
     *         this monitor when its {@link AlertLevel#WARN}.
     */
    @Override
    public long getWarnLogTimeInterval() {
        return _warnLogTimeInterval;
    }

    /**
     * Set the interval between successive log entries for this monitor when its
     * {@link AlertLevel#WARN}.
     * 
     * @param warnLogTimeInterval
     *            the interval in milliseconds
     */
    @Override
    public void setWarnLogTimeInterval(long warnLogTimeInterval) {
        Util.rangeCheck(warnLogTimeInterval, MINIMUM_WARN_INTERVAL, MAXIMUM_WARN_INTERVAL);
        _warnLogTimeInterval = warnLogTimeInterval;
    }

    /**
     * @return the interval in milliseconds between successive log entries for
     *         this monitor when its {@link AlertLevel#ERROR}.
     */
    @Override
    public long getErrorLogTimeInterval() {
        return _errorLogTimeInterval;
    }

    /**
     * Set the interval between successive log entries for this monitor when its
     * {@link AlertLevel#ERROR}.
     * 
     * @param _warnLogTimeInterval
     *            the interval in milliseconds
     */
    @Override
    public void setErrorLogTimeInterval(long errorLogTimeInterval) {
        Util.rangeCheck(errorLogTimeInterval, MINIMUM_ERROR_INTERVAL, MAXIMUM_ERROR_INTERVAL);
        _errorLogTimeInterval = errorLogTimeInterval;
    }

    /**
     * @return the number of events per category on which to keep a complete
     *         history.
     */
    @Override
    public int getHistoryLength() {
        return _historyLength;
    }

    /**
     * @param name
     *            Category name
     * @return the History for that category
     */
    public synchronized History getHistory(String name) {
        return _historyMap.get(name);
    }

    /**
     * Set the number of events per category on which to keep a complete
     * history. Once the number of events exceeds this count, the events
     * aggregated.
     * 
     * @param historyLength
     *            the historyLength to set
     */
    @Override
    public void setHistoryLength(int historyLength) {
        Util.rangeCheck(historyLength, MINIMUM_HISTORY_LENGTH, MAXIMUM_HISTORY_LENGTH);
        _historyLength = historyLength;
    }

    /**
     * Called periodically to emit any pending log messages
     */
    @Override
    public synchronized void poll(final boolean force) {
        for (final History history : _historyMap.values()) {
            history.poll(System.currentTimeMillis(), force);
        }
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, History> entry : _historyMap.entrySet()) {
            sb.append(String.format("%12s: %s\n", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    @Override
    public String getSummary() {
        return toString();
    }

    /**
     * @return a detailed history report for the specified category
     * @param select
     *            The category name to include, optionally with wildcards '*'
     *            and '?'
     */
    @Override
    public synchronized String getDetailedHistory(final String select) {
        Pattern pattern = Util.pattern(select, true);
        StringBuilder sb = new StringBuilder();
        for (final Map.Entry<String, History> entry : _historyMap.entrySet()) {
            if (pattern.matcher(entry.getKey()).matches()) {
                sb.append(String.format("%s:\n", entry.getKey()));
                sb.append(entry.getValue().getDetailedHistory());
            }
        }
        return sb.toString();
    }

    @Override
    public synchronized String getAlertLevel() {
        AlertLevel level = AlertLevel.NORMAL;
        for (final Map.Entry<String, History> entry : _historyMap.entrySet()) {
            if (entry.getValue().getLevel().compareTo(level) > 0) {
                level = entry.getValue().getLevel();
            }
        }
        return level.toString();
    }

    @Override
    public MBeanNotificationInfo[] getNotificationInfo() {
        String[] types = new String[] { NOTIFICATION_TYPE };
        String name = Notification.class.getName();
        String description = "Alert raised by Akiban PersistIT";
        MBeanNotificationInfo info = new MBeanNotificationInfo(types, name, description);
        return new MBeanNotificationInfo[] { info };
    }

    /**
     * Emits a log message. If there has been only one Event, log it as a
     * standard log message. If there have been multiple Events, emit a
     * recurring event message. Subclasses may replace this implementation.
     * 
     * @param history
     */
    protected void log(History history) {
        final Event event = history.getLastEvent();
        if (event != null && event.getLogItem().isEnabled()) {
            if (history.getCount() == 1) {
                event.getLogItem().log(event.getArgs());
            } else {
                event.getLogItem().logRecurring(history.getCount(), history.getDuration(), event.getArgs());
            }
        }
    }

    /**
     * Broadcasts a JMX Notification. Subclasses may replace this
     * implementation.
     * 
     * @param history
     */
    protected void sendNotification(History history) {
        final Event event = history.getLastEvent();
        if (event != null && event.getLogItem().isEnabled()) {
            final String description = LogBase.recurring(event.getLogItem().logMessage(event.getArgs()), history
                    .getCount(), history.getDuration());
            Notification notification = new Notification(NOTIFICATION_TYPE, getClass().getName(), _notificationSequence.incrementAndGet(),
                    description);
            sendNotification(notification);
        }
    }

    /**
     * <p>
     * Format an event object as a String. By default this method returns a
     * String containing the time of the event formatted as a compact Date/Time
     * conversion followed by the event arguments concatenated in a
     * comma-limited list. For example,
     * 
     * <code><pre>
     *    2012-01-13 16:52:05 SomeException, SomeInfo1, SomeInfo2
     * </pre></code>
     * 
     * for an event that happened on March 13, 2012 at 4:25:05pm for which there
     * were three argument elements.
     * </p>
     * <p>
     * A subclass may override this method to provide a more readable version.
     * </p>
     * 
     * @param event
     * @return
     */
    protected String format(Event event) {
        return event == null ? "null" : event.toString();
    }

    /**
     * Return the context-sensitive level of the event. By default this method
     * returns the level supplied when the event was posted. A subclass can
     * override this method to compute a different level based on context; for
     * example, a result determined by an aggregated history.
     * 
     * @param category
     *            category returned by {@link #categorize(Event)}
     * @param event
     *            the event being posted
     * @param level
     *            the original <code>Level</code>
     * @param history
     *            the <code>History</code> for this category
     * @return translated Level
     */
    protected AlertLevel translateLevel(Event event, AlertLevel level, History history) {
        return level;
    }

    /**
     * Return a category name for the event. By default this is simply the name
     * of the alert. A subclass can override this method to build separate
     * histories for different event categories, e.g., different kinds of
     * Exceptions.
     * 
     * @param event
     * @param level
     * @return
     */
    protected String translateCategory(final Event event, String category) {
        return category;
    }

    /**
     * @return Map of all History instances by category name
     */
    protected synchronized Map<String, History> getHistoryMap() {
        return new TreeMap<String, History>(_historyMap);
    }
}
