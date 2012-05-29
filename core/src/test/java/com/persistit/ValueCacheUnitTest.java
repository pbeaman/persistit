/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
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

import junit.framework.Assert;

import org.junit.Test;

import com.persistit.Value.ValueCache;

/**
 * 
 * @version 1.0
 */
public class ValueCacheUnitTest {

    @Test
    public void test1() {
        System.out.print("test1 ");
        final ValueCache vc = new ValueCache();
        for (int handle = 0; handle < 100; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
            assertEquals(-1, vc.put(handle, o));
            assertEquals(handle, vc.put(handle, o));
        }
        for (int handle = 0; handle < 100; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
        }
        for (int handle = 0; handle < 100; handle++) {
            final Object o = vc.get(handle);
            assertEquals(handle, vc.lookup(o));
        }

        System.out.println("- done");
    }

    @Test
    public void test2() {
        System.out.print("test2 ");
        final ValueCache vc = new ValueCache();
        for (int handle = 0; handle < 5000; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
            assertEquals(-1, vc.put(handle, o));
            assertEquals(handle, vc.put(handle, o));
        }
        for (int handle = 0; handle < 5000; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
        }
        for (int handle = 0; handle < 5000; handle++) {
            final Object o = vc.get(handle);
            assertEquals(handle, vc.lookup(o));
        }
        System.out.println("- done");
    }

    @Test
    public void test3() {
        System.out.print("test3 ");
        final long ts = System.currentTimeMillis();
        final ValueCache vc = new ValueCache();

        for (int cycle = 0; cycle < 10; cycle++) {
            vc.clear();
            for (int handle = 0; handle < 1000; handle++) {
                final Object o = new Integer(handle);
                assertEquals(-1, vc.lookup(o));
                assertEquals(-1, vc.put(handle, o));
                assertEquals(handle, vc.put(handle, o));
            }
            for (int handle = 0; handle < 1000; handle++) {
                final Object o = new Integer(handle);
                assertEquals(-1, vc.lookup(o));
            }
            for (int handle = 0; handle < 1000; handle++) {
                final Object o = vc.get(handle);
                assertEquals(handle, vc.lookup(o));
            }
        }
        System.out.print(" - " + (System.currentTimeMillis() - ts) + " ");
        System.out.println("- done");
    }

    public static void assertEquals(final int a, final int b) {
        if (a != b) {
            Assert.assertEquals(a, b);
        }
    }

    public static void main(final String[] args) throws Exception {

        runTest(args);
    }

    public static void runTest(final String[] args) throws Exception {
        final ValueCacheUnitTest vcst = new ValueCacheUnitTest();
        vcst.test1();
        vcst.test2();
        vcst.test3();
    }

}
