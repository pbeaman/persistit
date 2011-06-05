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

import java.io.File;

import org.junit.Test;

import com.persistit.CLI;
import com.persistit.Management;
import com.persistit.PersistitMap;


public class CommandLineTest extends PersistitUnitTestCase {

    @Test
    public void testCliParser() throws Exception {
        assertNotNull(CLI.parseTask(_persistit, "backup file=somefile -a -y -z"));
        assertNotNull(CLI.parseTask(_persistit, "save trees=persistit file=somefile"));
        assertNotNull(CLI.parseTask(_persistit, "load trees=persistit:*{1:2} file=somefile -t -n"));
        assertNull(CLI.parseTask(_persistit, "open datapath=somefile"));
        try {
            CLI.parseTask(_persistit, "backup file=somefile -s -y -z wrong=parameter");
            fail();
        } catch (Exception e) {
            // okay
        }
    }

    @Test
    public void testCommands() throws Exception {

        final PersistitMap<Integer, String> pmap = new PersistitMap<Integer, String>(
                _persistit.getExchange("persistit", "CommandLineTest",
                        true));
        for (int index = 0; index < 500; index++) {
            pmap.put(new Integer(index), "This is the record for index="
                    + index);
        }

        final Management management = _persistit.getManagement();

        String status = management
                .launch("icheck trees=persistit:CommandLineTest");
        waitForCompletion(taskId(status));
        final File file = File.createTempFile("CommandLineTest", ".sav");
        file.deleteOnExit();
        status = management.launch("save file=" + file
                + " trees=persistit:CommandLineTest{200:}");
        waitForCompletion(taskId(status));
        pmap.clear();

        status = management.launch("load file=" + file);
        waitForCompletion(taskId(status));

        assertEquals(300, pmap.size());
    }

    private long taskId(final String status) {
        return Long.parseLong(status);
    }


    private void waitForCompletion(final long taskId) throws Exception {
        for (int waiting = 0; waiting < 20000; waiting++) {
            final String status = _persistit.getManagement().execute(
                    "task taskId=" + taskId);
            if (status.endsWith("done")) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Task " + taskId
                + " did not compelete within 10 seconds");
    }

    @Override
    public void runAllTests() throws Exception {

    }

}
