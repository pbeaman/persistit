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
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.persistit.Management;
import com.persistit.ManagementCommand;
import com.persistit.PersistitMap;
import com.persistit.TaskCheck;

public class ManagementCommandTest extends PersistitUnitTestCase {

    @Test
    public void testManagementCommandParser() throws Exception {

        ManagementCommand m = ManagementCommand.parse("task taskId=12345 -v");
        verify(m, "[taskId=12345, -v]");
        m = ManagementCommand.parse("&task&taskId=12345&-v");
        verify(m, "[taskId=12345, -v]");
        m = ManagementCommand.parse("-task-taskId=12345-\\-v");
        verify(m, "[taskId=12345, -v]");
        m = ManagementCommand
                .parse("--------------------task-taskId=12345-\\-v--------------------");
        verify(m, "[taskId=12345, -v]");
    }

    private void verify(final ManagementCommand m, final String expected) {
        assertEquals(TaskCheck.class, m.getTaskClass());
        assertEquals(expected, Arrays.asList(m.getArgs()).toString());
    }

    @Test
    public void testCommands() throws Exception {

        final PersistitMap<Integer, String> pmap = new PersistitMap<Integer, String>(
                _persistit.getExchange("persistit", "ManagementCommandTest",
                        true));
        for (int index = 0; index < 500; index++) {
            pmap.put(new Integer(index), "This is the record for index="
                    + index);
        }

        final Management management = _persistit.getManagement();

        String status = management
                .launch("icheck trees=persistit,ManagementCommandTest");
        waitForCompletion(taskId(status));
        final File file = File.createTempFile("ManagementCommandTest", ".sav");
        file.deleteOnExit();
        status = management.launch("save file=" + file
                + " keyfilter={200:} trees=persistit,ManagementCommandTest");
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
        for (int waiting = 0; waiting < 20; waiting++) {
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
