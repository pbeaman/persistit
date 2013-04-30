/**
 * Copyright 2011-2012 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.persistit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.ref.WeakReference;
import java.util.List;

import org.junit.Test;

import com.persistit.CleanupManager.CleanupAction;
import com.persistit.exception.PersistitException;

public class CleanupManagerTest extends PersistitUnitTestCase {

    volatile int _counter = 0;
    volatile int _last = 0;

    private CleanupManager cm() {
        return _persistit.getCleanupManager();
    }

    private class CleanupMockAction implements CleanupAction {
        final int _sequence;

        CleanupMockAction(final int sequence) {
            _sequence = sequence;
        }

        @Override
        public int compareTo(final CleanupAction action) {
            return _sequence - ((CleanupMockAction) action)._sequence;

        }

        @Override
        public void performAction(final Persistit persistit, final List<CleanupAction> consequentActions)
                throws PersistitException {
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
