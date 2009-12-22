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
 * Created on Apr 6, 2004
 */
package com.persistit.unit;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import com.persistit.PersistitMap;
import com.persistit.StreamLoader;
import com.persistit.StreamSaver;
import com.persistit.exception.PersistitException;

public class SaveLoadTest1 extends PersistitTestCase {
    
    public void test1() throws PersistitException, IOException {
        System.out.print("test1 ");
        final TreeMap tmap = new TreeMap();
        final PersistitMap pmap =
            new PersistitMap(_persistit.getExchange("persistit", "SaveLoadTest1", true));
        for (int index = 0; index < 500; index++) {
            tmap.put(new Integer(index), "This is the record for index="
                + index);
        }
        pmap.clear();
        pmap.putAll(tmap);

        final File saveFile = File.createTempFile("SaveLoadTest", ".sav");
        final StreamSaver saver = new StreamSaver(_persistit, saveFile);
        saver.saveTrees("persistit", new String[] { "SaveLoadTest1" });
        saver.close();
        pmap.clear();
        final StreamLoader loader = new StreamLoader(_persistit, saveFile);
        loader.load();
        final boolean comparison = pmap.equals(tmap);
        assertTrue(comparison);
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new SaveLoadTest1().initAndRunTest();
    }

    public void runTest() throws Exception {
        test1();
    }

}
