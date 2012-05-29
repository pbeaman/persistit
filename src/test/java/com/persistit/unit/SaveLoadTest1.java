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

package com.persistit.unit;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.TreeMap;

import org.junit.Test;

import com.persistit.PersistitMap;
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
