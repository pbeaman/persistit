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

package com.persistit.unit;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.junit.Test;

import com.persistit.DefaultCoderManager;
import com.persistit.exception.PersistitException;

public class DefaultCoderManagerTest extends PersistitUnitTestCase implements
        Serializable {

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

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit,
                "**"));
        assertTrue(cm.isSerialOverride(Number.class));
        assertTrue(cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class));
        assertTrue(cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit,
                "java.lang.*,java.util.*"));
        assertTrue(cm.isSerialOverride(Number.class));
        assertTrue(cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class));
        assertTrue(!cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit,
                "java.lang.**,java.util.*"));
        assertTrue(cm.isSerialOverride(Number.class));
        assertTrue(cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class)); // because it's
        // not
        // Serializable
        assertTrue(!cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit,
                "java.**"));
        assertTrue(cm.isSerialOverride(Number.class));
        assertTrue(cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class)); // because it's
        // not
        // Serializable
        assertTrue(cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit,
                "java.**Map"));
        assertTrue(!cm.isSerialOverride(Number.class));
        assertTrue(!cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class));
        assertTrue(!cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm = new DefaultCoderManager(_persistit,
                "java.util.*," + DefaultCoderManagerTest.class.getName()));

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

    public void runAllTests() throws Exception {
        test1();
    }
}
