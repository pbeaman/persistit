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

import java.io.File;
import java.util.Properties;

public class UnitTestProperties {

    public final static String DATA_PATH = "/tmp/persistit_test_data";
    public final static String VOLUME_NAME = "persistit";

    /**
     * Returns a Properties object with settings appropriate for unit tests -
     * i.e., very small allocations.
     * 
     * @return Initialization properties for unit tests.
     */
    public static Properties getProperties(final boolean cleanup) {
        if (cleanup) {
            cleanUpDirectory(new File(DATA_PATH));
        }
        final Properties p = new Properties();
        p.setProperty("datapath", DATA_PATH);
        p.setProperty("buffer.count.16384", "20");
        p.setProperty("volume.1", "${datapath}/" + VOLUME_NAME + ",create,"
                + "pageSize:16384,initialPages:100,extensionPages:100," + "maximumPages:25000");
        p.setProperty("journalpath", "${datapath}/persistit_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("tmpvoldir", "${datapath}");
        p.setProperty("jmx", "true");
        return p;
    }

    public static Properties getBiggerProperties(final boolean cleanup) {
        if (cleanup) {
            cleanUpDirectory(new File(DATA_PATH));
        }
        final Properties p = new Properties();
        p.setProperty("datapath", DATA_PATH);
        p.setProperty("buffer.count.16384", "2000");
        p.setProperty("volume.1", "${datapath}/" + VOLUME_NAME + ",create,"
                + "pageSize:16384,initialPages:100,extensionPages:100," + "maximumPages:100000,alias:persistit");
        p.setProperty("volume.2", "${datapath}/persistit_system,create,"
                + "pageSize:16384,initialPages:100,extensionPages:100," + "maximumPages:100000,alias:_system");
        p.setProperty("volume.3", "${datapath}/persistit_txn,create,"
                + "pageSize:16384,initialPages:100,extensionPages:100," + "maximumPages:100000,alias:_txn");
        p.setProperty("journalpath", "${datapath}/persistit_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("tmpvoldir", "${datapath}");
        return p;
    }

    public static Properties getAlternateProperties(final boolean cleanup) {
        if (cleanup) {
            cleanUpDirectory(new File(DATA_PATH));
        }
        final Properties p = new Properties();
        p.setProperty("datapath", DATA_PATH);
        p.setProperty("buffer.count.16384", "40");
        p.setProperty("volume.1", "${datapath}/temp,create," + "pageSize:16384,initialPages:4,extensionPages:1,"
                + "maximumPages:100000,alias:persistit");
        p.setProperty("journalpath", "${datapath}/persistit_alt_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("tmpvoldir", "${datapath}");
        return p;
    }

    public static Properties getPropertiesByMemory(final boolean cleanup, final String memSpecification) {
        if (cleanup) {
            cleanUpDirectory(new File(DATA_PATH));
        }
        final Properties p = new Properties();
        p.setProperty("datapath", DATA_PATH);
        p.setProperty("buffer.memory.16384", memSpecification);
        p.setProperty("volume.1", "${datapath}/" + VOLUME_NAME + ",create,"
                + "pageSize:16384,initialPages:100,extensionPages:100," + "maximumPages:25000");
        p.setProperty("journalpath", "${datapath}/persistit_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("tmpvoldir", "${datapath}");
        return p;
    }

    public final static void cleanUpDirectory(final File file) {
        if (!file.exists()) {
            file.mkdirs();
            return;
        } else if (file.isFile()) {
            throw new IllegalStateException(file + " must be a directory");
        } else {
            final File[] files = file.listFiles();
            cleanUpFiles(files);
        }
    }

    public final static void cleanUpFiles(final File[] files) {
        for (final File file : files) {
            if (file.isDirectory()) {
                cleanUpDirectory(file);
            }
            file.delete();
        }
    }
}
