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
 * Created on Aug 24, 2004
 */
package com.persistit.unit;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.junit.Test;

import com.persistit.DefaultCoderManager;
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

        _persistit.setCoderManager(cm =
            new DefaultCoderManager(_persistit, "java.lang.*,java.util.*"));
        assertTrue(cm.isSerialOverride(Number.class));
        assertTrue(cm.isSerialOverride(String.class));
        assertTrue(cm.isSerialOverride(HashMap.class));
        assertTrue(!cm.isSerialOverride(Constructor.class));
        assertTrue(!cm.isSerialOverride(SimpleDateFormat.class));
        assertTrue(!cm.isSerialOverride(DefaultCoderManagerTest.class));

        _persistit.setCoderManager(cm =
            new DefaultCoderManager(_persistit, "java.lang.**,java.util.*"));
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

        _persistit.setCoderManager(cm =
            new DefaultCoderManager(_persistit, "java.util.*,"
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
    
    public void runAllTests() throws Exception {
        test1();
    }
}
