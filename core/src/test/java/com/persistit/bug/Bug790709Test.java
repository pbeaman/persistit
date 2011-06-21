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

package com.persistit.bug;

import org.junit.Test;

import com.persistit.Buffer;
import com.persistit.Exchange;
import com.persistit.TestShim;
import com.persistit.unit.PersistitUnitTestCase;

/**
 * Bug 790709 This happened on-site at XXX with the halo release. The server
 * process was running with assertions enabled. The DELETE statement that failed
 * was: DELETE FROM online_search where profileID = NAME_CONST('iUID',22162961)
 * <p />
 * The trace that showed up in the server log was:
 * 
 * <code><pre>
 * 
 * The trace that showed up in the server log was:
 * 
 * WARN [Network-Worker-Thread-17] 2011-05-27 19:17:11,110 DefaultRequestHandler.java (line 70) Caught class java.lang.AssertionError on execution of DeleteRowRequest(18) rowData=online_search(22162961,'N','Y','Y','Y','N','N','N','N','N','N','N','Y','N','Y','N','N','N','Y','Y','Y','Y','Y','Y','N','N','N','Y','N','N','N','N','N','N','N','N','N','N','N','N','N','N','Y','N','N','N','N','N','N','N','N','N','Y','N','N','N','N','N','N','N','N','N','Y','Y','N','N','N','N','N','Y','N','N','N','N','N','Y','N','N','N','Y','N','N','N','N','N','N','N','Y','N','N','N','Y','N','N','Y','N','N','N','N','N','Y','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N','N',176,18,,,'0','2011-05-23 08:23:41','1','1','',1,2000435160,2000435248,755240,51.2666667000,0.5166667000,3,'N','N','Jakebarrett',79,42,62,8900192270,,,1)
 * java.lang.AssertionError
 *         at com.persistit.FastIndex.putEbc(FastIndex.java:66)
 *         at com.persistit.FastIndex.recompute(FastIndex.java:137)
 *         at com.persistit.Buffer.findKey(Buffer.java:758)
 *         at com.persistit.Exchange.searchLevel(Exchange.java:112
 *         ...
 * </pre></code>
 * <p />
 * The cause is a page left with more than _maxKeys keys in it, and that happens
 * in REBALANCE case in Buffer.join(). There is no code in the rebalance case to
 * limit the rebalanced pages to the maximum number of keys.
 * <p />
 * 
 */
public class Bug790709Test extends PersistitUnitTestCase {

    /*
     * This test fails prior to bug fix.
     */
    @Test
    public void testRebalancePages1() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "Bug790709Test", true);
        final int maxKeys = TestShim.maxKeys(ex.getVolume());
        //
        // Create maxKeys keys in each of two pages. (There are
        // two because when we add the maxKey+1th key, the page
        // must split.
        //
        // On the second page the keys are a tad longer. The
        // asymmetry is
        //
        for (int i = 0; i <= maxKeys; i++) {
            ex.clear().append("a").append(i).store();
        }

        for (int i = 0; i <= maxKeys; i++) {
            ex.clear().append("b").append(i).append("x").store();
        }

        ex.clear().append("a");
        Buffer buffer1 = ex.fetchBufferCopy(0);
        // System.out.println(buffer1 + " keys=" + buffer1.getKeyCount());
        assertTrue(buffer1.getKeyCount() <= maxKeys);
        ex.append("z");
        Buffer buffer2 = ex.fetchBufferCopy(0);
        // System.out.println(buffer2 + " keys=" + buffer2.getKeyCount());
        assertTrue(buffer2.getKeyCount() <= maxKeys);

        buffer2.getRecords()[0].getKeyState().copyTo(ex.getKey());
        ex.remove();

        ex.clear().append("a");
        buffer1 = ex.fetchBufferCopy(0);
        // System.out.println(buffer1 + " keys=" + buffer1.getKeyCount());
        assertTrue(buffer1.getKeyCount() <= maxKeys);
        ex.append("z");
        buffer2 = ex.fetchBufferCopy(0);
        // System.out.println(buffer2 + " keys=" + buffer2.getKeyCount());
        assertTrue(buffer2.getKeyCount() <= maxKeys);
        assertNotSame(buffer1, buffer2);
    }

    /*
     * This test succeeds prior to bug fix.
     */
    @Test
    public void testRebalancePages2() throws Exception {
        Exchange ex = _persistit.getExchange("persistit", "Bug790709Test", true);
        final int maxKeys = TestShim.maxKeys(ex.getVolume());
        //
        // Create maxKeys keys in each of two pages. (There are
        // two because when we add the maxKey+1th key, the page
        // must split.
        //
        // On the second page the keys are a tad longer. The
        // asymmetry is
        //
        for (int i = 0; i <= maxKeys; i++) {
            ex.clear().append("a").append(i).append("x").store();
        }

        for (int i = 0; i <= maxKeys; i++) {
            ex.clear().append("b").append(i).store();
        }

        ex.clear().append("a");
        Buffer buffer1 = ex.fetchBufferCopy(0);
        System.out.println(buffer1 + " keys=" + buffer1.getKeyCount());
        assertTrue(buffer1.getKeyCount() <= maxKeys);
        ex.append("z");
        Buffer buffer2 = ex.fetchBufferCopy(0);
        System.out.println(buffer2 + " keys=" + buffer2.getKeyCount());
        assertTrue(buffer2.getKeyCount() <= maxKeys);

        buffer2.getRecords()[0].getKeyState().copyTo(ex.getKey());
        ex.remove();

        ex.clear().append("a");
        buffer1 = ex.fetchBufferCopy(0);
        System.out.println(buffer1 + " keys=" + buffer1.getKeyCount());
        assertTrue(buffer1.getKeyCount() <= maxKeys);
        ex.append("z");
        buffer2 = ex.fetchBufferCopy(0);
        System.out.println(buffer2 + " keys=" + buffer2.getKeyCount());
        assertTrue(buffer2.getKeyCount() <= maxKeys);
        assertNotSame(buffer1, buffer2);
    }

    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub

    }

}
