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
     * @param writePageSleepInterval the writePageSleepInterval to set
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
     * Path for file into which IO events should be logged, or <tt>null</tt>
     * to disable IO logging.
     * @param toFile
     * @throws IOException
     */
    public void setLogFile(final String toFile) throws IOException;
    
    /**
     * Path for file into which IO events should be logged, or <tt>null</tt>
     * if IO logging is disabled.
     * @return
     */
    public String getLogFile();
}
