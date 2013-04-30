/**
 * Copyright 2012 Akiban Technologies, Inc.
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

package com.persistit;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

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
        final long createTime = volume.getStatistics().getCreateTime();
        volume.close();
        final Volume reopened = _persistit.loadVolume(volume.getSpecification());
        final long createTimeReopened = reopened.getStatistics().getCreateTime();
        assertEquals("Create time should be preserved when a volume is closed", createTime, createTimeReopened);

    }

}
