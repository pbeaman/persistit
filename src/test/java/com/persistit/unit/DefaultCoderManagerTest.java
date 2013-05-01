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
