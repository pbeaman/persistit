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

import java.io.IOException;

import javax.management.MXBean;

@MXBean
public interface IOMeterMXBean {

    public final static String MXBEAN_NAME = "com.persistit:type=Persistit,class=IOMeter";

    /**
     * @return the writePageSleepInterval
     */
    public long getWritePageSleepInterval();

    /**
     * @param writePageSleepInterval
     *            the writePageSleepInterval to set
     */
    public void setWritePageSleepInterval(long writePageSleepInterval);

    /**
     * Time interval in milliseconds between page copy operations.
     * 
     * @return the CopySleepInterval
     */
    public long getCopyPageSleepInterval();

    /**
     * @param copyPageSleepInterval
     *            the copySleepInterval to set
     */
    public void setCopyPageSleepInterval(long copyPageSleepInterval);

    /**
     * @return the quiescentIOthreshold
     */
    public long getQuiescentIOthreshold();

    /**
     * @param quiescentIOthreshold
     *            the quiescentIOthreshold to set
     */
    public void setQuiescentIOthreshold(long quiescentIO);

    /**
     * @return the ioRate
     */
    public long getIoRate();

    /**
     * Path for file into which IO events should be logged, or <code>null</code>
     * to disable IO logging.
     * 
     * @param toFile
     * @throws IOException
     */
    public void setLogFile(final String toFile) throws IOException;

    /**
     * Path for file into which IO events should be logged, or <code>null</code>
     * if IO logging is disabled.
     * 
     * @return
     */
    public String getLogFile();
}
