/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
