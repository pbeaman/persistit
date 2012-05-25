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

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.ref.WeakReference;

import org.junit.Test;

import com.persistit.CleanupManager.CleanupAction;
import com.persistit.exception.PersistitException;
import com.persistit.unit.PersistitUnitTestCase;

public class CleanupManagerTest extends PersistitUnitTestCase {

    volatile int _counter = 0;
    volatile int _last = 0;

    private CleanupManager cm() {
        return _persistit.getCleanupManager();
    }

    private class CleanupMockAction implements CleanupAction {
        final int _sequence;

        CleanupMockAction(int sequence) {
            _sequence = sequence;
        }

        @Override
        public int compareTo(CleanupAction action) {
            return _sequence - ((CleanupMockAction) action)._sequence;

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

    @Test
    public void testCleanupHappens() throws Exception {
        for (int i = 1; i <= 500; i++) {
            cm().offer(new CleanupMockAction(i));
        }
        cm().setPollInterval(100);
        for (int i = 0; i < 10 && cm().getEnqueuedCount() > 0; i++) {
            Thread.sleep(1000);
        }
        assertEquals(500, _counter);
        assertEquals(1, cm().getErrorCount());
        assertEquals(499, cm().getPerformedCount());
    }

    @Test
    public void testOverflow() throws Exception {
        for (int i = 1; i <= CleanupManager.DEFAULT_QUEUE_SIZE * 2; i++) {
            cm().offer(new CleanupMockAction(i));
        }

        assertTrue(cm().getAcceptedCount() > 0);
        assertTrue(cm().getRefusedCount() > 0);
        final String s = cm().toString();
        assertTrue(s.contains("CleanupMockAction("));
        cm().clear();
        assertEquals(0, cm().getEnqueuedCount());
    }

    @Test
    public void testMemoryReleasedOnCrash() throws Exception {
        final WeakReference<Persistit> ref = new WeakReference<Persistit>(_persistit);
        CleanupManager cm = cm();
        cm.offer(new CleanupMockAction(1));
        _persistit.crash();
        _persistit = new Persistit();
        cm = null;
        assertTrue(doesRefBecomeNull(ref));
    }
}
