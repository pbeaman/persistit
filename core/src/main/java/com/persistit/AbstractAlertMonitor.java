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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.persistit.AbstractAlertMonitor.Level;
import com.persistit.logging.PersistitLevel;
import com.persistit.logging.PersistitLogMessage.LogItem;

/**
 * Manage the process of accumulating and logging of abnormal events such as
 * IOExceptions and measurements outside of expected thresholds. A concrete
 * AbstractAlertMonitor implementation is set up and registered as an MXBean
 * during Persistit initialization, and its behavior can be modified through the
 * MXBean interface.
 * 
 * @author peter
 * 
 */

interface AlertMonitor {
    /**
     * @return the name of this AlertMonitor
     */
    String getName();

    /**
     * @return the current alert level of this monitor
     */
    Level getLevel();

    /**
     * Restore this alert monitor to its initial state at level
     * {@link Level#NORMAL} with no history.
     */
    void reset();

    /**
     * @return the interval in milliseconds between successive log entries for
     *         this monitor when its {@link Level#WARN}.
     */
    long getWarnLogTimeInterval();

    /**
     * Set the interval between successive log entries for this monitor when its
     * {@link Level#WARN}.
     * 
     * @param warnLogTimeInterval
     *            the interval in milliseconds
     */
    void setWarnLogTimeInterval(long warnLogTimeInterval);

    /**
     * @return the interval in milliseconds between successive log entries for
     *         this monitor when its {@link Level#ERROR}.
     */
    long getErrorLogTimeInterval();

    /**
     * Set the interval between successive log entries for this monitor when its
     * {@link Level#ERROR}.
     * 
     * @param _warnLogTimeInterval
     *            the interval in milliseconds
     */
    void setErrorLogTimeInterval(long errorLogTimeInterval);

    /**
     * @return the number of events per category on which to keep a complete
     *         history.
     */
    long getHistoryLength();

    /**
     * Set the number of events per category on which to keep a complete
     * history. Once the number of events exceeds this count, the events
     * aggregated.
     * 
     * @param historyLength
     *            the historyLength to set
     */
    void setHistoryLength(int historyLength);

}

public abstract class AbstractAlertMonitor implements AlertMonitor {

    enum Level {
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

    public class History {
        long _first;
        long _last;
        List<EventTime> _history;
        int _count;
        long _minimum;
        long _maximum;
        long _total;
        Object _extra;

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
         * @return the first
         */
        public long getFirst() {
            return _first;
        }

        /**
         * @return the last
         */
        public long getLast() {
            return _last;
        }

        /**
         * @return the history
         */
        public List<EventTime> getHistory() {
            return _history;
        }

        /**
         * @param count
         *            the count to set
         */
        public void setCount(int count) {
            _count = count;
        }

    }

    public class EventTime {
        final Object _event;
        final long _time;

        public EventTime(final Object event, long time) {
            _event = event;
            _time = time;
        }
    }

    private final String _name;
    private Level _level = Level.NORMAL;
    private volatile long _warnLogTimeInterval;
    private volatile long _errorLogTimeInterval;
    private volatile int _historyLength;
    private volatile long _lastWarnLogTime;
    private volatile long _lastErrorLogTime;
    private final LogItem _logItem;
    private final Map<String, History> _historyMap = new TreeMap<String, History>();

    protected AbstractAlertMonitor(final String name, final LogItem logItem) {
        _name = name;
        _logItem = logItem;
    }

    /**
     * Post an event. The event can be an Exception, a String, or other kind of
     * object understood by the {@link #categorize(Object, Level)} method. Does
     * nothing if <code>level</code> is {@link Level#NORMAL}.
     * 
     * @param event
     *            A description of the event as an Exception, a String or other
     *            object
     * @param level
     *            Indicates whether this event is a warning or an error.
     * @param time
     *            at which event occurred
     */
    protected final synchronized void post(Object event, Level level, long time) {
        final String category = categorize(event);
        History history = _historyMap.get(category);
        if (history == null) {
            history = new History();
            _historyMap.put(category, history);
        }
        final Level translatedLevel = translateLevel(category, event, level, history, time);
        boolean logEvent = false;
        if (translatedLevel.compareTo(_level) > 0) {
            _level = translatedLevel;
            logEvent = true;
        }
        int size = Math.min(_historyLength, 1);
        while (history._history.size() >= size) {
            history._history.remove(0);
        }
        history._history.add(new EventTime(event, time));
        history._count++;
        history._first = Math.min(history._first, time);
        history._last = Math.max(history._last, time);
        aggregate(event, history, level, time);

        final long now = System.currentTimeMillis();

        switch (_level) {
        case ERROR:
            if (now > _lastErrorLogTime + _errorLogTimeInterval) {
                _errorLogTimeInterval = now;
                logEvent = true;
            }
            break;
        case WARN:
            if (now > _lastWarnLogTime + _warnLogTimeInterval) {
                _lastWarnLogTime = now;
                logEvent = true;
            }
            break;

        default:
            // Ignore the NORMAL case
        }

        if (logEvent) {
            _logItem.log(logLevel(translatedLevel), logArguments(translatedLevel, event, history, time));
        }

    }

    /**
     * Return the context-sensitive level of the event. By default this method
     * returns the level supplied when the event was posted. A subclass can
     * override this method to compute a different level based on context; for
     * example, a result determined by aggregated history.
     * 
     * @param category
     *            category returned by {@link #categorize(Object)}
     * @param event
     *            the event being posted
     * @param level
     *            the original <code>Level</code>
     * @param history
     *            the <code>History</code> for this category
     * @param time
     *            system time at which this event occurred.
     * @return translated Level
     */
    protected Level translateLevel(String category, Object event, Level level, History history, long time) {
        return level;
    }

    /**
     * Perform extended aggregation of the history. For example, a subclass may
     * extend this method the maintain the minimum, maximum and total fields of
     * the History object. By default this method does nothing.
     * 
     * @param event
     * @param history
     * @param level
     * @param time
     */
    protected void aggregate(Object event, History history, Level level, long time) {
        // Default: do nothing
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
    protected String categorize(Object event) {
        return _name;
    }

    protected PersistitLevel logLevel(final Level level) {
        switch (level) {
        case ERROR:
            return PersistitLevel.ERROR;
        case WARN:
            return PersistitLevel.WARNING;
        default:
            return PersistitLevel.NONE;
        }
    }

    protected Object[] logArguments(final Level level, final Object event, final History history, final long time) {
        return new Object[] { event };
    }

    /**
     * @return the name of this AlertMonitor
     */
    @Override
    public String getName() {
        return _name;
    }

    /**
     * @return the current alert level of this monitor
     */
    @Override
    public synchronized Level getLevel() {
        return _level;
    }

    /**
     * Restore this alert monitor to its initial state at level
     * {@link Level#NORMAL} with no history.
     */
    @Override
    public synchronized void reset() {
        _level = Level.NORMAL;
    }

    /**
     * @return the interval in milliseconds between successive log entries for
     *         this monitor when its {@link Level#WARN}.
     */
    @Override
    public long getWarnLogTimeInterval() {
        return _warnLogTimeInterval;
    }

    /**
     * Set the interval between successive log entries for this monitor when its
     * {@link Level#WARN}.
     * 
     * @param warnLogTimeInterval
     *            the interval in milliseconds
     */
    @Override
    public void setWarnLogTimeInterval(long warnLogTimeInterval) {
        _warnLogTimeInterval = warnLogTimeInterval;
    }

    /**
     * @return the interval in milliseconds between successive log entries for
     *         this monitor when its {@link Level#ERROR}.
     */
    @Override
    public long getErrorLogTimeInterval() {
        return _errorLogTimeInterval;
    }

    /**
     * Set the interval between successive log entries for this monitor when its
     * {@link Level#ERROR}.
     * 
     * @param _warnLogTimeInterval
     *            the interval in milliseconds
     */
    @Override
    public void setErrorLogTimeInterval(long errorLogTimeInterval) {
        _errorLogTimeInterval = errorLogTimeInterval;
    }

    /**
     * @return the number of events per category on which to keep a complete
     *         history.
     */
    @Override
    public long getHistoryLength() {
        return _historyLength;
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
        _historyLength = historyLength;
    }

}
