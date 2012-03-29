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

package com.persistit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


/**
 * Accumulates and reports IOExceptions occurring in background threads. This
 * class attempts to distinguish between Disk Full and other types of
 * IOExceptions. Unfortunately there is no standard way to determine the
 * underlying cause of the IOException so the message is interpreted as a
 * category.
 * 
 * @author peter
 */
public final class IOAlertMonitor extends AbstractAlertMonitor implements IOAlertMonitorMXBean {
    private final static String NAME = "IOAlertMonitor";
    

    private final Persistit _persistit;

    public IOAlertMonitor(final Persistit persistit) {
        super(NAME);
        _persistit = persistit;
    }
    
    @Override
    protected String categorize(Event event) {
        if (event.getLead() instanceof IOException) {
            return ((IOException)event.getLead()).getMessage();
        } else {
            return super.categorize(event);
        }
    }

    @Override
    protected void log(AlertLevel level, String category, Event event, History history, long time) {
        if (history.getCount() == 1) {
            _persistit.getLogBase().journalWriteError.log(event.getArgs());
        } else {
            List<Object> args = Arrays.asList(event.getArgs());
            args.add(history.getCount());
            _persistit.getLogBase().recurringJournalWriteError.log(args.toArray());
        }
    }
    
}
