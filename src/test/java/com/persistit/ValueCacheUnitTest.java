/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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
