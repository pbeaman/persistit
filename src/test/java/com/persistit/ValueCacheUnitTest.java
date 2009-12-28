/*
 * Copyright (c) 2004 Persistit Corporation. All Rights Reserved.
 *
 * The Java source code is the confidential and proprietary information
 * of Persistit Corporation ("Confidential Information"). You shall
 * not disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Persistit Corporation.
 *
 * PERSISTIT CORPORATION MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE
 * SUITABILITY OF THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR
 * A PARTICULAR PURPOSE, OR NON-INFRINGEMENT. PERSISTIT CORPORATION SHALL
 * NOT BE LIABLE FOR ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING,
 * MODIFYING OR DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 * 
 * Created on Jul 24, 2004
 */
package com.persistit;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.persistit.Value.ValueCache;

/**
 * 
 * @version 1.0
 */
public class ValueCacheUnitTest extends TestCase {
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

    public void test2() {
        System.out.print("test2 ");
        final ValueCache vc = new ValueCache();
        for (int handle = 0; handle < 50000; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
            assertEquals(-1, vc.put(handle, o));
            assertEquals(handle, vc.put(handle, o));
        }
        for (int handle = 0; handle < 50000; handle++) {
            final Object o = new Integer(handle);
            assertEquals(-1, vc.lookup(o));
        }
        for (int handle = 0; handle < 50000; handle++) {
            final Object o = vc.get(handle);
            assertEquals(handle, vc.lookup(o));
        }
        System.out.println("- done");
    }

    public void test3() {
        System.out.print("test3 ");
        final long ts = System.currentTimeMillis();
        final ValueCache vc = new ValueCache();

        for (int cycle = 0; cycle < 100; cycle++) {
            vc.clear();
            for (int handle = 0; handle < 10000; handle++) {
                final Object o = new Integer(handle);
                assertEquals(-1, vc.lookup(o));
                assertEquals(-1, vc.put(handle, o));
                assertEquals(handle, vc.put(handle, o));
            }
            for (int handle = 0; handle < 10000; handle++) {
                final Object o = new Integer(handle);
                assertEquals(-1, vc.lookup(o));
            }
            for (int handle = 0; handle < 10000; handle++) {
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
