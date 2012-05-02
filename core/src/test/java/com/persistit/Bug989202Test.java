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

package com.persistit;

import org.junit.Test;

import com.persistit.unit.PersistitUnitTestCase;

;

/**
 * Persistit rev: 284
 * 
 * The volume header has a field to store the creation time. This isn't used for
 * anything directly but is displays through various interfaces (management,
 * JMX). Other than being a nice attribute for reference, having the value be
 * real would make for a nice sanity check when debugging various issues (could
 * compare to journal creation times, etc).
 * 
 * The creation time is saved in the statistics structure upon first creation
 * (VolumeStorageT2()#create() -> #truncate()), but the value isn't copied into
 * the header area after that (#flushMetaData() -> updateMetaData()).
 */
public class Bug989202Test extends PersistitUnitTestCase {

    @Test
    public void testVolumeCreateTime() throws Exception {
        final Volume volume = _persistit.getVolume("persistit");
        long createTime = volume.getStatistics().getCreateTime();
        volume.close();
        Volume reopened = _persistit.loadVolume(volume.getSpecification());
        long createTimeReopened = reopened.getStatistics().getCreateTime();
        assertEquals("Create time should be preserved when a volume is closed", createTime, createTimeReopened);

    }

}
