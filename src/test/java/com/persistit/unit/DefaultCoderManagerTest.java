/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
 */

package com.persistit.unit;

import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.junit.Test;

import com.persistit.DefaultCoderManager;
import com.persistit.PersistitUnitTestCase;
import com.persistit.exception.PersistitException;

public class DefaultCoderManagerTest extends PersistitUnitTestCase implements Serializable {

    private static final long serialVersionUID = 1L;

    String _volumeName = "persistit";

    @Test
    public void test1() throws PersistitException {
        // Tests join calculation.
        //
        System.out.print("test1 ");

        DefaultCoderManager cm;

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit));
        assertTrue(!cm.isSerialOverride(Number.class));
        assertTrue(!cm.isSerialOverride(String.class));
        assertTrue(!cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class));
        assertTrue(!cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit, "**"));
        assertTrue(cm.isSerialOverride(Number.class));
        assertTrue(cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class));
        assertTrue(cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit, "java.lang.*,java.util.*"));
        assertTrue(cm.isSerialOverride(Number.class));
        assertTrue(cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class));
        assertTrue(!cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit, "java.lang.**,java.util.*"));
        assertTrue(cm.isSerialOverride(Number.class));
        assertTrue(cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class)); // because it's
        // not
        // Serializable
        assertTrue(!cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit, "java.**"));
        assertTrue(cm.isSerialOverride(Number.class));
        assertTrue(cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class)); // because it's
        // not
        // Serializable
        assertTrue(cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit, "java.**Map"));
        assertTrue(!cm.isSerialOverride(Number.class));
        assertTrue(!cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class));
        assertTrue(!cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit, "java.util.*,"
                + DefaultCoderManagerTest.class.getName()));

        assertTrue(!cm.isSerialOverride(Number.class));
        assertTrue(!cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class));
        assertTrue(!cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(new DefaultCoderManager(_persistit));

        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new DefaultCoderManagerTest().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
    }
}
