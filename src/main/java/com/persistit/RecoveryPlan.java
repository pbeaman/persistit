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
 */

package com.persistit;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import com.persistit.exception.CorruptPrewriteJournalException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.RecoveryMissingVolumesException;

class RecoveryPlan
{

    // A structure to hold a complete plan for recovering committed, but
    // unwritten buffers.
    //
    public final static int STATUS_NOT_LOADED = 0;
    public final static int STATUS_NO_PWJ_FILE = 1;
    public final static int STATUS_EMPTY_PWJ_FILE = 2;
    public final static int STATUS_CORRUPT_PWJ_FILE = 3;
    public final static int STATUS_VALID_PWJ_FILE = 4;
    
    public final static byte[] NULL_HEADER =
        new byte[PrewriteJournal.BUFFER_SIGNATURE.length];
    
    private final static int UNUSED = 0;
    private final static int PARTIAL = 1;
    private final static int COMMITTED = 2;
    private final static int DONE = 3;
    
    private HashMap _volumeInfoById = new HashMap();
    
    private final static byte[][] VISIBLE_STATUS_ARRAY =
    {
        PrewriteJournal.VISIBLE_UNUSED,
        PrewriteJournal.VISIBLE_PARTIAL,
        PrewriteJournal.VISIBLE_COMMITTED,
        PrewriteJournal.VISIBLE_DONE,
    };
    
    private final static int MAX_PWJB_COUNT = PrewriteJournal.MAXIMUM_PWJB_COUNT;
    private boolean _verbose;
    private ArrayList[] _pwjbPlans = new ArrayList[MAX_PWJB_COUNT];
    private long[] _pwjbGenerations = new long[MAX_PWJB_COUNT];
    private long[] _pwjbUpdateTimes = new long[MAX_PWJB_COUNT];
    private int[] _pwjbStatus = new int[MAX_PWJB_COUNT];
    private RandomAccessFile _raf;
    private int _status;
    private long _pwjFileAddr;
    private int _pwjbCount = 1;     // will get corrected below
    private int _pwjbSize = 0;      // will get corrected
    private long _recoveryGeneration = Integer.MAX_VALUE;   // will get corrected
    private int _recoveryCount = 0;
    private final Persistit _persistit;
    
    public RecoveryPlan(final Persistit persistit) {
    	_persistit = persistit;
    }
    
    public void open(String pwjFileName)
    throws PersistitException
    {
        try
        {
            if (!(new File(pwjFileName).exists()))
            {
                _status = STATUS_NO_PWJ_FILE;
                return;
            } 
            _raf = new RandomAccessFile(pwjFileName, "r");
            
            if (_raf.length() < PrewriteJournal.HEADER_RECORD_SIZE)
            {
                _status = STATUS_EMPTY_PWJ_FILE;
                return;
            }
            
            byte[] bytes = new byte[PrewriteJournal.HEADER_RECORD_SIZE];
            
            _status = STATUS_CORRUPT_PWJ_FILE;
            
            for (int pwjbIndex = 0; pwjbIndex < _pwjbCount; pwjbIndex++)
            {
                _pwjFileAddr = (long)pwjbIndex * (long)_pwjbSize;
                _raf.seek(_pwjFileAddr);
                int length = _raf.read(bytes); 
                if (length < PrewriteJournal.HEADER_RECORD_SIZE)
                {
                    corrupt("truncated header record", 0);
                }
                
                if (Util.equalsByteSubarray(
                        bytes,
                        PrewriteJournal.HEADER_SIGNATURE,
                        NULL_HEADER))
                {
                    continue;
                }
                
                if (!Util.equalsByteSubarray(
                        bytes,
                        PrewriteJournal.HEADER_SIGNATURE,
                        PrewriteJournal.BUFFER_SIGNATURE))
                               
                {
                    corrupt("bad signature", 0);
                }
                
                if (!Util.equalsByteSubarray(
                        bytes,
                        PrewriteJournal.HEADER_VISIBLE_INDEX,
                        PrewriteJournal.visibleIndex(pwjbIndex)))
                {
                    corrupt("invalid index value", 0);
                }
                
                int size = Util.getInt(
                    bytes, PrewriteJournal.HEADER_SIZE);
                    
                int count = Util.getInt(
                    bytes, PrewriteJournal.HEADER_PWJB_COUNT);
                    
                long generation = Util.getLong(
                    bytes, PrewriteJournal.HEADER_GENERATION);
                    
                long updateTime = Util.getLong(
                    bytes, PrewriteJournal.HEADER_UPDATE_TIME);
                    
                if (count < 1 ||
                    count > PrewriteJournal.MAXIMUM_PWJB_COUNT ||
                    (pwjbIndex != 0 && count != _pwjbCount))
                {
                    corrupt("invalid buffer count " + count, 0);
                }
                
                if (size < PrewriteJournal.MINIMUM_PWJB_SIZE ||
                    size > PrewriteJournal.MAXIMUM_PWJB_SIZE ||
                    (pwjbIndex != 0 && size != _pwjbSize))
                {
                    corrupt("invalid pwj buffer size " + size, 0);
                }
                
                if (pwjbIndex == 0)
                {
                    _recoveryGeneration = generation;
                    _pwjbCount = count;
                    _pwjbSize = size;
                }
                
                //
                // Now figure out the buffer's status:
                //
                int pwjbStatus;
                for (pwjbStatus = VISIBLE_STATUS_ARRAY.length;
                     --pwjbStatus >= 0;)
                {
                    if (Util.equalsByteSubarray(
                            bytes,
                            PrewriteJournal.HEADER_STATUS,
                            VISIBLE_STATUS_ARRAY[pwjbStatus]))
                    {
                        break;
                    }
                }
                
                if (pwjbStatus < 0)
                {
                    corrupt("invalid status ", 0);
                }
                
                _pwjbUpdateTimes[pwjbIndex] = updateTime;
                _pwjbGenerations[pwjbIndex] = generation;
                _pwjbStatus[pwjbIndex] = pwjbStatus;
                if (pwjbStatus == COMMITTED && _recoveryGeneration > generation)
                {
                    _recoveryGeneration = generation;
                }
                
                if (_verbose)
                {
                    System.out.println(
                        "PWJ Generation " + generation + 
                        " status=" + pwjbStatus +
                        " updateTime=" + updateTime +
                        " size=" + size);
                }
            }
            //
            // If we found a COMMITTED generation, then we have work to do.
            // Now we walk through all the buffers starting from the earliest
            // COMMITTED generation and process them.
            //
            if (_recoveryGeneration < Integer.MAX_VALUE || _verbose)
            {
                for (int index = 0; index < _pwjbCount; index++)
                {
                    int pwjbIndex =
                        (int)((_recoveryGeneration + index) % _pwjbCount);
                    long generation = _pwjbGenerations[pwjbIndex];
                    if (generation < _recoveryGeneration && !_verbose)
                    {
                        // We have wrapped to an earlier generation -
                        // all done.
                        break;
                    }

                    _pwjFileAddr = (long)pwjbIndex * (long)_pwjbSize;
                    _raf.seek(_pwjFileAddr);
                    int length = _raf.read(bytes); 

                    if (_pwjbStatus[pwjbIndex] == COMMITTED ||
                        _pwjbStatus[pwjbIndex] == DONE)
                    {
                        _raf.seek(_pwjFileAddr);
                        bytes = new byte[_pwjbSize];
                        _raf.read(bytes, 0, _pwjbSize);
                        
                        // There were buffers committed to the prewrite 
                        // journal that either have or have not been
                        // written to the database.  We build a list:
                        //
                        int bufferCount =
                            Util.getInt(bytes, PrewriteJournal.HEADER_BUFFER_COUNT);
                            
                        int volumeCount =
                            Util.getInt(bytes, PrewriteJournal.HEADER_VOLUME_COUNT);
    
                        int next = PrewriteJournal.HEADER_RECORD_SIZE;
                                                
                        for (int volumeIndex = 0;
                             volumeIndex < volumeCount;
                             volumeIndex++)
                        {
                            
                            try
                            {                            
                                int nextNext =
                                    Volume.confirmMetaData(bytes, next);
                                    
                                long volumeId =
                                    Volume.idFromMetaData(bytes, next);
                                    
                                int volumeBufferSize =
                                    Volume.bufferSizeFromMetaData(bytes, next);
                                    
                                String pathName =
                                    Volume.pathNameFromMetaData(bytes, next);
                                    
                                Long idKey = new Long(volumeId);
                                
                                VolumeInfo volumeInfo =
                                    (VolumeInfo)_volumeInfoById.get(idKey);
                                    
                                if (volumeInfo == null)
                                {
                                    volumeInfo = new VolumeInfo(
                                        volumeId, pathName, volumeBufferSize);
                                    _volumeInfoById.put(idKey, volumeInfo);
                                }
                                
                                else if (!volumeInfo._pathName.equals(pathName) ||
                                         volumeInfo._bufferSize != volumeBufferSize)
                                {
                                    corrupt("volume id " + volumeId + 
                                            " maps to both " + volumeInfo._pathName +
                                            " and " + pathName,
                                            next);
                                }
                                
                                next = nextNext;
                            }
                            catch (PersistitException lex)
                            {
                                // This is fatal because the prewrite journal
                                // is malformed.
                                corrupt("Bad volume metadata record " + lex, next);
                            }
                        }
                        
                        for (int bufferIndex = 0;
                             bufferIndex < bufferCount;
                             bufferIndex++)
                        {
                            if (!Util.equalsByteSubarray(
                                    bytes,
                                    next,
                                    PrewriteJournal.RECORD_SIGNATURE))
                            {
                                throw new CorruptPrewriteJournalException(
                                    "Invalid buffer record signature at next " +
                                    (next - 1));
                            }
                            next += PrewriteJournal.RECORD_SIGNATURE.length;
                            int bufferSize = Util.getInt(bytes, next);
                            next += 4;
                    
                            long id = Util.getLong(bytes, next);
                            next += 8;
                    
                            long page = Util.getLong(bytes, next);
                            next += 8;
                    
                            long timeWritten = Util.getLong(bytes, next);
                            next += 8;
                            
                            RecoveryPlanElement element = new RecoveryPlanElement();
                            
                            VolumeInfo volumeInfo =
                                (VolumeInfo)_volumeInfoById.get(new Long(id));
                            
                            element._volumeInfo = volumeInfo;
                            element._pageAddr = page;
                            element._bufferSize = bufferSize;
                            element._originalTimeWritten = timeWritten;
                            element._pwjFileAddr = _pwjFileAddr + next;
                            //
                            // If this is a DONE generation then this page
                            // has already been written to the Volume and will
                            // not be rewritten.
                            //
                            if (_pwjbStatus[pwjbIndex] == DONE)
                            {
                                element._written = true;
                            }
                            else
                            {
                                _recoveryCount++;
                            }
                            
                            ArrayList list = _pwjbPlans[pwjbIndex];
                            
                            if (list == null)
                            {
                                list = new ArrayList(100);
                                _pwjbPlans[pwjbIndex] = list;
                            }
                            list.add(element);
                            
                            next += bufferSize;
                            
                            markEarlierGenerationWritten(generation, volumeInfo, page);
                        }
                    }
                }
            }
            _status = STATUS_VALID_PWJ_FILE;
        }
        catch (IOException ioe)
        {
            throw new PersistitIOException(ioe);
        }
    }
    
    private void markEarlierGenerationWritten(long laterGeneration, VolumeInfo volumeInfo, long page)
    {
        for (long generation = laterGeneration;
             --generation >= _recoveryGeneration;)
        {
            int pwjbIndex = (int)(generation % _pwjbCount);
            if (_pwjbGenerations[pwjbIndex] != generation) continue;
            
            // No need to mark any page in a DONE generation because it
            // is already known to have been writtten.
            if (_pwjbStatus[pwjbIndex] == DONE) continue;
            
            ArrayList plan = _pwjbPlans[pwjbIndex];
            if (plan == null || plan.size() == 0) continue;
            //
            // Here we are scanning a COMMITTED generation that is earlier
            // than the supplied generation.  If we find a reference to the same
            // page as has been noted in a later generation, we mark the earlier
            // element written.  This is so that we don't overwrite
            // already-written later generation pages with earlier copies.
            //
            // We can use == on the VolumeInfo because we have made VolumeInfo
            // objects unique using the _volumeInfoById Map.  We could rely
            // on the ordering of elements in the list (they should be strictly
            // increasing by volume name and page number), but for reliability
            // we just do a plain old linear search. This happens quite rarely.
            //
            for (int index = 0; index < plan.size(); index++)
            {
                RecoveryPlanElement element = (RecoveryPlanElement)plan.get(index);
                if (element._volumeInfo == volumeInfo &&
                    element._pageAddr == page)
                {
                    //
                    // Since a later version of this page was already written to
                    // the Volume (or will be, then we mark this earlier version.
                    //
                    if (!element._written)
                    {
                        _recoveryCount--;
                        element._written = true;
                    }
                }
            }
        } 
    }
    
    public void close()
    {
        try
        {
            if (_raf != null)
            {
                _raf.close();
            } 
            _raf = null;
    
            for (Iterator iter = _volumeInfoById.values().iterator();
                 iter.hasNext();)
            {
                VolumeInfo volumeInfo = (VolumeInfo)iter.next();
                if (volumeInfo._raf != null)
                {
                    volumeInfo._raf.close();
                    volumeInfo._raf = null;
                }
            }
            _volumeInfoById.clear();
            _status = 0;
            _recoveryGeneration = 0;
            _pwjFileAddr = 0;
        }
        catch (IOException ioe)
        {
        	_persistit.getLogBase().log(LogBase.LOG_EXCEPTION, LogBase.detailString(ioe));
        }
        
    }
        
    private void corrupt(String reason, long next)
    throws CorruptPrewriteJournalException
    {
        _status = STATUS_CORRUPT_PWJ_FILE;
        throw new CorruptPrewriteJournalException(
            reason + " at file address " + (_pwjFileAddr + next));
    }
    
    public void dump(PrintWriter pw)
    {
        if (_recoveryGeneration == Integer.MAX_VALUE)
        {
            pw.println("No recovery required");
        }
        else for (int i = 0; i < _pwjbCount; i++)
        {
            long gen = _pwjbGenerations[i];
            int status = _pwjbStatus[i];
            ArrayList list = _pwjbPlans[i];
            int size = list == null ? 0 : list.size();
            pw.println("Prewrite journal buffer #" + i + 
                       ": gen=" + gen + " status=" + status + 
                       " size=" + size);
                       
            for (int j = 0; j < size; j++)
            {
                RecoveryPlanElement element = (RecoveryPlanElement)list.get(j);
                
                pw.println(Util.format(i, 4) + " " + element);
            }
        }
        pw.flush();
    }
    public void dump(PrintStream ps)
    {
        dump(new PrintWriter(ps));
    }
    
    public String dump()
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.println();
        pw.println("Recovery Plan");
        pw.println();
        dump(pw);
        pw.println();
        return sw.toString();
    }

    public int getStatus()
    {
        return _status;    
    }
    
    public boolean isHealthy()
    {
        return _status == STATUS_NO_PWJ_FILE ||
               _status == STATUS_EMPTY_PWJ_FILE ||
               _status == STATUS_VALID_PWJ_FILE;
    }
    
    public boolean hasUncommittedPages()
    {
        return _recoveryCount > 0;
    }
    
    public boolean commit(boolean okWithMissingFiles)
    throws PersistitIOException, RecoveryMissingVolumesException
    {
        try
        {
            // First we attempt to open all the volumes.
            //
            int missingFiles = 0;
            for (Iterator iter = _volumeInfoById.values().iterator();
                 iter.hasNext(); )
            {
                VolumeInfo volumeInfo = (VolumeInfo)iter.next();
                File file = new File(volumeInfo._pathName);
                if (!file.exists() || !file.isFile())
                {
                	_persistit.getLogBase().log(LogBase.LOG_RECOVERY_FAILURE1, volumeInfo._pathName);
                    missingFiles ++;
                }
                else
                {
                    try
                    {
                        volumeInfo._raf = new RandomAccessFile(file, "rw");
                    }
                    catch (FileNotFoundException fnfe)
                    {
                        throw new IllegalStateException(
                            "File exists but not found: " +
                            volumeInfo._pathName);
                    }
                }
            }
            if (missingFiles == 0 || okWithMissingFiles)
            {
                byte[] bytes = new byte[Buffer.MAX_BUFFER_SIZE];
    
                long nextGeneration = -1;
                int startIndex = -1;
                //
                // Find the lowest committed generation
                //
                for (int i = 0; i < _pwjbCount; i++)
                {
                    if (_pwjbPlans[i] != null && _pwjbPlans[i].size() > 0)
                    {
                        if (nextGeneration < 0 ||
                            nextGeneration > _pwjbGenerations[i])
                        {
                            nextGeneration = _pwjbGenerations[i];
                            startIndex = i;
                        }
                    }
                }
                            
                //
                // For each PrewriteJournalBuffer starting with the lowest
                // committed generation, commit any pages that were logged
                // as being fully committed in that buffer.
                //
                for (int i = 0; i < _pwjbCount; i++)
                {
                    int index = (i + startIndex) % _pwjbCount;
                    
                    ArrayList list = _pwjbPlans[index];
                    int size = list == null ? 0 : list.size();
                    for (int j = 0; j < size; j++)
                    {
                        RecoveryPlanElement element =
                            (RecoveryPlanElement)list.get(j);
                        if (element._volumeInfo._raf != null)
                        {
                            element.commit(_raf, bytes);
                        }
                    }
                }
            }
            else
            {
                throw new RecoveryMissingVolumesException();
            }
            return true;
        }
        catch (IOException ioe)
        {
            throw new PersistitIOException(ioe);
        }
    }
    
    private static class VolumeInfo
    {
        private String _pathName;
        private long _id;
        private int _bufferSize;
        private RandomAccessFile _raf;
        
        VolumeInfo(long id, String pathName, int bufferSize)
        {
            _id = id;
            _pathName = pathName;
            _bufferSize = bufferSize;
        }
        
        public String toString()
        {
            return _pathName;
        }
    }

    private final class RecoveryPlanElement
    {
        private VolumeInfo _volumeInfo;
        private long _pageAddr;
        private int _bufferSize;
        private long _pwjFileAddr;
        private long _originalTimeWritten;
        private boolean _written;
        
        void commit(RandomAccessFile raf, byte[] buffer)
        throws IOException
        {
            // If this page has already been written then do nothing.
            if (_written) return;
            
            int size = _volumeInfo._bufferSize;
            raf.seek(_pwjFileAddr);
            int length = raf.read(buffer, 0, size);
            if (length != size) throw new IOException(
                "Invalid file read length " + length + " -- should be " + size);
            if (_volumeInfo._raf == null)
            {
                _persistit.getLogBase().log(LogBase.LOG_RECOVERY_FAILURE2,
                    _pageAddr, 0, 0, 0, 0,
                    _volumeInfo._pathName, null, null, null, null);
            }
            else
            {
                _volumeInfo._raf.seek(_pageAddr * size);
                _volumeInfo._raf.write(buffer, 0, size);
                _written = true;
            }
        }
        
        public String toString()
        {
            return
                "Page=" + _pageAddr + " fileAddr=" + _pwjFileAddr +
                " time=" + _originalTimeWritten + " to be written=" + !_written;
        }
    }
    
    private final static String[] ARGS_TEMPLATE =
    {
        "pwjpath|String:|Prewrite journal file",
        "_flag|m|Okay to perform recovery with missing Volumes",
        "_flag|v|Verbose mode",
    };
    

    public static void main(String[] args)
    throws Exception
    {
        ArgParser ap = new ArgParser("RecoveryPlan", args, ARGS_TEMPLATE);
        if (ap.isUsageOnly()) return;
        
        String pwjPath = ap.getStringValue("pwjpath");
        
        RecoveryPlan plan = new RecoveryPlan(new Persistit());
        if (ap.isFlag('v')) plan._verbose = true;
        try
        {
            plan.open(pwjPath);
        }
        catch (CorruptPrewriteJournalException cpje)
        {
            cpje.printStackTrace();
            System.err.println();
        }
        
        plan.dump(System.out);
        
        plan.commit(ap.isFlag('m'));
        plan.close();

    }

}
