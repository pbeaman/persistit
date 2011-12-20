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

import com.persistit.CleanupManager.CleanupAction;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

public class CleanupManagerTest extends PersistitUnitTestCase {

    volatile int _counter = 0;
    volatile int _last = 0;
    CleanupManager _cm;
    
    private class CleanupMockAction implements CleanupAction {
        final int _sequence;
        
        CleanupMockAction(int sequence) {
            _sequence = sequence;
        }
        
        @Override
        public int compareTo(CleanupAction action) {
            return _sequence - ((CleanupMockAction)action)._sequence;
            
        }

        @Override
        public void performAction(Persistit persistit) throws PersistitException {
            assertEquals(_last + 1, _sequence);
            _last = _sequence;
            _counter++;
            if (_sequence == 123) {
                throw new ExpectedException();
            }
        }
        
        @Override
        public String toString() {
            return "CleanupMockAction(" + _sequence + ")";
        }
        
    }

    private static class ExpectedException extends PersistitException {

        private static final long serialVersionUID = 1L;
        
    }
    
    public void setUp() throws Exception {
        super.setUp();
        _cm = _persistit.getCleanupManager();
    }
    
    
    public void testCleanupHappens() throws Exception {
        for (int i = 1; i <= 500; i++) {
            _cm.offer(new CleanupMockAction(i));
        }
        _cm.setPollInterval(100);
        for (int i = 0; i < 10 && _cm.getEnqueuedCount() > 0; i++) {
            Thread.sleep(1000);
        }
        assertEquals(500, _counter);
        assertEquals(1, _cm.getErrorCount());
        assertEquals(499, _cm.getPerformedCount());
    }
    
    public void testOverflow() throws Exception {
        for (int i = 1; i <= CleanupManager.DEFAULT_QUEUE_SIZE * 2; i++) {
            _cm.offer(new CleanupMockAction(i));
        }
        
        assertTrue(_cm.getAcceptedCount() > 0);
        assertTrue(_cm.getRefusedCount()> 0);
        final String s = _cm.toString();
        assertTrue(s.contains("CleanupMockAction("));
        _cm.clear();
        assertEquals(0, _cm.getEnqueuedCount());
        
    }
}
