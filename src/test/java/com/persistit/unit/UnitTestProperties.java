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

import java.util.Properties;

public class UnitTestProperties {
    /**
     * Returns a Properties object with settings appropriate for unit tests -
     * i.e., very small allocations.
     * 
     * @return Initialization properties for unit tests.
     */
    public static Properties getProperties() {
        final Properties p = new Properties();
        p.setProperty("datapath", "/data");
        p.setProperty("buffer.count.8192", "20");
        p.setProperty("volume.1", "${datapath}/persistit.v01,create,"
            + "pageSize:8192,initialPages:1,extensionPages:1,"
            + "maximumPages:10000");
        p.setProperty("pwjpath", "${datapath}/persistit_unit_test.pwj");
        p.setProperty("pwjdelete", "true");
        p.setProperty("pwjsize", "128K");
        p.setProperty("pwjcount", "1");
        return p;
    }

    public static Properties getBiggerProperties() {
        final Properties p = new Properties();
        p.setProperty("datapath", "/data");
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
        p.setProperty("pwjpath", "${datapath}/persistit_unit_test.pwj");
        p.setProperty("pwjdelete", "true");
        p.setProperty("pwjsize", "16M");
        p.setProperty("pwjcount", "2");
        return p;
    }

    public static Properties getAlternateProperties() {
        final Properties p = new Properties();
        p.setProperty("datapath", "/data");
        p.setProperty("buffer.count.8192", "40");
        p.setProperty("volume.1", "${datapath}/temp.v01,create,"
            + "pageSize:8192,initialPages:4,extensionPages:1,"
            + "maximumPages:100000,alias:persistit,temporary");
        p.setProperty("pwjpath", "${datapath}/persistit_alt_test.pwj");
        p.setProperty("pwjdelete", "true");
        p.setProperty("pwjsize", "1M");
        p.setProperty("pwjcount", "1");
        return p;
    }
}
