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

package com.persistit;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

public class StatisticsTaskTest extends PersistitUnitTestCase {

    @Test
    public void testStatisticsTask() throws Exception {
        final StatisticsTask task = new StatisticsTask();
        task.setPersistit(_persistit);
        task.setMessageStream(System.out);
        task.setup(1, "stats", "cls", 0, 5);
        final File file = File.createTempFile("statistics", ".log");
        file.deleteOnExit();
        task.setupTaskWithArgParser(new String[] {"-a", "-r", "delay=1", "count=5", "file=" + file.getAbsolutePath()});
        task.run();
        final BufferedReader reader = new BufferedReader(new FileReader(file));
        int lines = 0;
        String line;
        while ((line = reader.readLine()) != null) {
            lines++;
            System.out.println(line);
        }
        assertEquals(4, lines);
    }
    
    @Override
    public void runAllTests() throws Exception {
        // TODO Auto-generated method stub
        
    }
    
}
