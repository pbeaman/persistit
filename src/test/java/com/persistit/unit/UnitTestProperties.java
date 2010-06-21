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
 * Created on Aug 15, 2004
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
    public static Properties getProperties() {
        cleanUpDirectory(new File(DATA_PATH));
        final Properties p = new Properties();
        p.setProperty("datapath", DATA_PATH);
        p.setProperty("buffer.count.8192", "20");
        p.setProperty("volume.1", "${datapath}/persistit.v01,create,"
                + "pageSize:8192,initialPages:10,extensionPages:10,"
                + "maximumPages:10000");
        p.setProperty("logpath", "${datapath}/persistit_log");
        return p;
    }

    public static Properties getBiggerProperties() {
        cleanUpDirectory(new File(DATA_PATH));
        final Properties p = new Properties();
        p.setProperty("datapath", DATA_PATH);
        p.setProperty("buffer.count.8192", "4000");
        p.setProperty("volume.1", "${datapath}/persistit.v01,create,"
                + "pageSize:8192,initialPages:100,extensionPages:100,"
                + "maximumPages:100000,alias:persistit");
        p.setProperty("volume.2", "${datapath}/persistit_system.v01,create,"
                + "pageSize:8192,initialPages:100,extensionPages:100,"
                + "maximumPages:100000,alias:_system");
        p.setProperty("volume.3", "${datapath}/persistit_txn.v01,create,"
                + "pageSize:8192,initialPages:100,extensionPages:100,"
                + "maximumPages:100000,alias:_txn");
        p.setProperty("logpath", "${datapath}/persistit_log");
        return p;
    }

    public static Properties getAlternateProperties() {
        cleanUpDirectory(new File(DATA_PATH));
        final Properties p = new Properties();
        p.setProperty("datapath", DATA_PATH);
        p.setProperty("buffer.count.8192", "40");
        p.setProperty("volume.1", "${datapath}/temp.v01,create,"
                + "pageSize:8192,initialPages:4,extensionPages:1,"
                + "maximumPages:100000,alias:persistit,temporary");
        p.setProperty("logpath", "${datapath}/persistit_alt_log");
        return p;
    }

    private final static void cleanUpDirectory(final File file) {
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

    private final static void cleanUpFiles(final File[] files) {
        for (final File file : files) {
            if (file.isDirectory()) {
                cleanUpDirectory(file);
            }
            file.delete();
        }
    }
}
