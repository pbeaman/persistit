/**
 * Copyright © 2005-2012 Akiban Technologies, Inc.  All rights reserved.
 * 
 * This program and the accompanying materials are made available
 * under the terms of the Eclipse Public License v1.0 which
 * accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * 
 * This program may also be available under different license terms.
 * For more information, see www.akiban.com or contact licensing@akiban.com.
 * 
 * Contributors:
 * Akiban Technologies, Inc.
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
        p.setProperty("rmiport", System.getProperty("rmiport", "8081"));
        p.setProperty("jmx", "true");
        p.setProperty("bufferinventory", "/tmp/persistit_test_data");
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
        p.setProperty("volume.1", "${datapath}/temp,create," + "pageSize:16384,initialPages:4,extensionPages:1,"
                + "maximumPages:100000,alias:persistit");
        p.setProperty("journalpath", "${datapath}/persistit_alt_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("tmpvoldir", "${datapath}");
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
        p.setProperty("volume.1", "${datapath}/" + VOLUME_NAME + ",create,"
                + "pageSize:16384,initialPages:100,extensionPages:100," + "maximumPages:25000");
        p.setProperty("journalpath", "${datapath}/persistit_journal");
        p.setProperty("logfile", "${datapath}/persistit_${timestamp}.log");
        p.setProperty("tmpvoldir", "${datapath}");
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
