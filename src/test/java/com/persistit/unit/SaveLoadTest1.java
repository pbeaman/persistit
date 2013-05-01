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

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import org.junit.Test;

import com.persistit.PersistitMap;
import com.persistit.PersistitUnitTestCase;
import com.persistit.StreamLoader;
import com.persistit.StreamSaver;
import com.persistit.exception.PersistitException;

public class SaveLoadTest1 extends PersistitUnitTestCase {

    @Test
    public void test1() throws PersistitException, IOException {
        System.out.print("test1 ");
        final TreeMap<Integer, String> tmap = new TreeMap<Integer, String>();
        final PersistitMap<Integer, String> pmap = new PersistitMap<Integer, String>(_persistit.getExchange(
                "persistit", "SaveLoadTest1", true));
        for (int index = 0; index < 500; index++) {
            tmap.put(new Integer(index), "This is the record for index=" + index);
        }
        pmap.clear();
        pmap.putAll(tmap);

        final File saveFile = File.createTempFile("SaveLoadTest", ".sav");
        saveFile.deleteOnExit();
        final StreamSaver saver = new StreamSaver(_persistit, saveFile);
        saver.saveTrees("persistit", new String[] { "SaveLoadTest1" });
        saver.close();
        pmap.clear();
        final StreamLoader loader = new StreamLoader(_persistit, saveFile);
        loader.load(null, false, false);
        final boolean comparison = pmap.equals(tmap);
        assertTrue(comparison);
        System.out.println("- done");
    }

    public static void main(final String[] args) throws Exception {
        new SaveLoadTest1().initAndRunTest();
    }

    @Override
    public void runAllTests() throws Exception {
        test1();
    }

}
