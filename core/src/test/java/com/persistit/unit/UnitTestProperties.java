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
import java.util.Properties;

public class UnitTestProperties {

    public final static String DATA_PATH = "/tmp/persistit_test_data";

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
        p.setProperty("volume.1", "${datapath}/persistit.v01,create,"
                + "pageSize:16384,initialPages:100,extensionPages:100,"
                + "maximumPages:25000");
        p.setProperty("journalpath", "${datapath}/persistit_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("rmiport", System.getProperty("rmiport", "8081"));
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
        p.setProperty("volume.1", "${datapath}/persistit.v01,create,"
                + "pageSize:16384,initialPages:100,extensionPages:100,"
                + "maximumPages:100000,alias:persistit");
        p.setProperty("volume.2", "${datapath}/persistit_system.v01,create,"
                + "pageSize:16384,initialPages:100,extensionPages:100,"
                + "maximumPages:100000,alias:_system");
        p.setProperty("volume.3", "${datapath}/persistit_txn.v01,create,"
                + "pageSize:16384,initialPages:100,extensionPages:100,"
                + "maximumPages:100000,alias:_txn");
        p.setProperty(
                "volume.4",
                "${datapath}/persistit_transient.v01,createOnly,"
                        + "pageSize:16384,initialPages:1000000,extensionPages:1000000,"
                        + "maximumPages:1000000,alias:transient,transient");
        p.setProperty("journalpath", "${datapath}/persistit_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("rmiport", System.getProperty("rmiport", "8081"));
        return p;
    }

    public static Properties getAlternateProperties(final boolean cleanup) {
        if (cleanup) {
            cleanUpDirectory(new File(DATA_PATH));
        }
        final Properties p = new Properties();
        p.setProperty("datapath", DATA_PATH);
        p.setProperty("buffer.count.16384", "40");
        p.setProperty("volume.1", "${datapath}/temp.v01,create,"
                + "pageSize:16384,initialPages:4,extensionPages:1,"
                + "maximumPages:100000,alias:persistit,transient");
        p.setProperty("journalpath", "${datapath}/persistit_alt_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("rmiport", System.getProperty("rmiport", "8081"));
        return p;
    }

    public static Properties getPropertiesByMemory(final boolean cleanup, final String memSpecification) {
        if (cleanup) {
            cleanUpDirectory(new File(DATA_PATH));
        }
        final Properties p = new Properties();
        p.setProperty("datapath", DATA_PATH);
        p.setProperty("buffer.memory.16384", memSpecification);
        p.setProperty("volume.1", "${datapath}/persistit.v01,create,"
                + "pageSize:16384,initialPages:100,extensionPages:100,"
                + "maximumPages:25000");
        p.setProperty("journalpath", "${datapath}/persistit_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("rmiport", System.getProperty("rmiport", "8081"));
        p.setProperty("jmx", "true");
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
