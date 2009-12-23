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
import java.io.RandomAccessFile;
import java.util.ArrayList;

import com.persistit.exception.CorruptPrewriteJournalException;
import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.InvalidPageStructureException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.PrewriteJournalPathException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.RetryException;
import com.persistit.exception.TimeoutException;
import com.persistit.exception.VolumeClosedException;

/**
 * <p>
 * Manages an array of buffers used to hold page copies that are being
 * committed to the volume backing file.  There are up to eight separate
 * prewrite buffers, with one thread per buffer that is actually responsible
 * for writing the data.
 * </p>
 * <p> 
 * The write ordering is designed to guarantee that the updates performed by
 * any particular thread are temporally consistent.  That is, if a thread
 * performs any update U2 after performing a prior update U1, then upon 
 * recovery after system failure, if U2 is present in the database, so is U1.
 * </p>
 * <p>
 * Any thread that is about to change a page calls the reserve() method
 * to reserve space.  If the page does not already have a reservation
 * in the current prewrite journal buffers, the reserve method creates one.
 * If there is insufficient space to make that reservation then the reserve()
 * method notifies a waiting writer thread to start writing buffer contents
 * to disk, and then throws a {@link RetryException}.  The caller must then
 * relinquish all of its claims to all resources and retry the operation.
 * </p>
 * <p>
 * Each of the writer threads is responsible for writing all the pages that
 * were reserved to one prewrite journal buffer.  The writer thread does the
 * following:
 * <ol>
 * <li>copy all the dirty buffers that were reserved into that prewrite
 *     journal buffer.  These buffers get marked unreserved once this
 *     copy is performed.  After a buffer has been made unreserved
 *     any subsequent attempt to reserve it will be done in the next
 *     available PrewriteJournalBuffer generation.</li>
 * <li>write the prewrite journal buffer image to the prewrite journal file.</li>
 * <li>write a prewrite journal committed record to the prewrite journal file.</li>
 * <li>write the pages to the volume backing file.</li>
 * <li>write a prewrite journal completed record to the prewrite journal file.
 *     Upon normal shutdown, all prewrite images are marked completed, 
 *     indicating that no recovery processing is necessary.</li>
 * </ol>
 * </p><p>
 * The sequencing of these disk writes is important.  The prewrite buffer is
 * first written to the prewrite journal file, marked with status indication
 * of PARTIALLY_WRITTEN, and forced to disk.  Then the status is updated
 * to COMMITTED and forced to disk to indicate that the journal buffer 
 * has been written fully.  Once this write is done, the pages are 
 * fully committed -- that is, if the system is interrupted at this time,
 * then on restart the committed, but as yet unwritten pages are recovered.
 * Next the page copies that were transferred to the prewrite image buffer
 * are written to the database backing files. Finally, the status is updated
 * to DONE, indicating that all pages in the prewrite journal have also been
 * written to the database and that that the write cycle is complete.
 * </p><p>
 * The cycle for each prewrite journal thread is as follows:
 * <dl>
 * <dt>S</dt><dd>  Sleep.</dd>
 * <dt>C</dt><dd>  Copy all reserved pages (if there are none, go back to sleep).</dd>
 * <dt>P</dt><dd>  Write a PARTIAL status marker, plus the content of
 *                 the prewrite journal buffer to the prewrite journal file.</dd>
 * <dt>T</dt><dd>  Write a COMMITTED status marker to the prewrite journal file.</dd>
 * <dt>W</dt><dd>  Write the page images to the database.</dd>
 * <dt>D</dt><dd>  Write a COMPLETED status marker to the prewrite journal file
 *                 then go back to sleep.</dd>
 * </dl> 
 * </p><p>
 * Each cycle is assigned an ever-increasing generation number. There
 * generally are multiple prewrite journal threads to permit concurrent
 * database writing.  The generation number is incremented in one of two
 * places: if the {@link #reserve} operation finds a buffer full then it
 * kicks the writer thread for that buffer and increments the current
 * generation.  Otherwise, if the writer thread wakes up after a natural
 * sleep interval, it increments the generation number and begins the copy
 * and commitment processing. 
 * </p><p>
 * As a future enhancement, the write operations to a the volume file(s) could
 * be performed by multiple threads - i.e., we could employ multiple threads
 * per journal buffer.
 * </p><p>
 * The writer threads must obey the following constraints, where m and n are
 * generation numbers such that n > m:
 * <ol> 
 * <li> T(n) may not begin until T(m) is complete.  That is, a later generation
 *      may not write its COMMITTED marker until all earlier generations
 *      have finished writing theirs.</li>
 *  
 * <li> W(m) may not write any page P that is in the set of pages committed
 *      by T(n).  That is, if multiple generations contain unwritten copies
 *      of the same page, only the latest version may be written.</li>
 *  
 * <li> If T(n) commits while W(m) is writing page P, then W(n) must wait until
 *      W(m) has completed its write operation on page P before writing the
 *      updated version of P.  That is, that latest version of any page
 *      must be written last, and two threads may not concurrently write the
 *      same page.  (Assume that collisions are rare.)</li>
 * 
 * <li> Any attempt to read page P from the volume must check whether P is in
 *      the set of pages committed by T(n) for any generation n where W(n) is
 *      incomplete.  That is, the sole correct copy of a page may for a period
 *      of time reside in the prewrite journal buffer itself.  In this event
 *      the reader should recover it from the prewrite journal buffer rather
 *      than reading an obsolete version from the volume.</li>
 * </ol>
 * </p>
 * @author pbeaman
 * @version 1.0
 */
public class PrewriteJournal
extends SharedResource
{
    private final static boolean DEBUG_ALWAYS_NEW_RETRY_EXCEPTION = false;
    private final static boolean DEBUG_POPULATE_TIME_FIELDS = false;
    
    private final static boolean INITIALIZE_TO_ZEROES = true;
    private final static byte[] ZEROES = new byte[1024];
    private final static long DEFAULT_WRITER_POLL_INTERVAL = 5000; // 30 sec
    private final static long SHORT_POLL_INTERVAL = 50;             // .05 sec
    private final static long SHORT_DELAY_INTERVAL = 1000;          // .05 sec
    private final static long DEFAULT_CLOSE_TIMEOUT = 600000;       // 60 sec
    private final static long ERROR_RETRY_INTERVAL = 5000;          // 5 sec
    private final static long MAX_ERROR_RETRY_INTERVAL = 60000;     // 1 minute
    private final static int MAX_JOURNAL_WRITE_SIZE = 8 * 1024 * 1024;
    /**
     * Overhead bytes per buffer
     */
    private final static int BUFFER_HEADER_SIZE = 64;
            
    /**
     * Overhead bytes per page 
     */
    private final static int RECORD_HEADER_SIZE = 32;

    /**
     * Default prewrite image buffer size = 4MB
     */
    public final static int MINIMUM_PWJB_SIZE = 64 * 1024;
    public final static int DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;
    public final static int MAXIMUM_PWJB_SIZE = Integer.MAX_VALUE;

    public final static int DEFAULT_BUFFER_COUNT = 2;
    public final static int MINIMUM_PWJB_COUNT = 1;
    public final static int MAXIMUM_PWJB_COUNT = 8;
    
    // Structure of PrewriteJournalBuffer header record
    //
    final static int HEADER_RECORD_SIZE =         64;
    final static int HEADER_SIGNATURE =            0;
    final static int HEADER_VISIBLE_INDEX =        4;
    final static int HEADER_STATUS =               8;
    final static int HEADER_PWJB_COUNT =          12;
    final static int HEADER_NEXT_AVAIL =          16;
    final static int HEADER_SIZE =                20;
    final static int HEADER_BUFFER_COUNT =        24;
    final static int HEADER_VOLUME_COUNT =        28;
    final static int HEADER_GENERATION =          32;
    final static int HEADER_OPEN_TIME =           40;
    final static int HEADER_UPDATE_TIME =         48;
    final static int HEADER_VERSION =             56;
    final static int HEADER_UNUSED =              60;
    
    final static int EMPTY = 0;
    final static int OPEN = 1;
    final static int CLOSED = 2;
    final static int COPYING = 3;
    final static int COMMITTING = 4;
    final static int WRITING = 5;
    final static int WRITTEN = 6;
    final static int SHUTDOWN = -1;
    
    final static String[] STATE_NAMES =
    {
        "EMPTY", 
        "OPEN", 
        "CLOSED", 
        "COPYING", 
        "COMMITTING",
        "WRITING", 
        "DONE",
    };
            
    final static byte[] BUFFER_SIGNATURE  = "PRWJ".getBytes();
    final static byte[] RECORD_SIGNATURE  = "PAGE".getBytes();
    
    final static byte[] VISIBLE_UNUSED    = "NOTU".getBytes();
    final static byte[] VISIBLE_PARTIAL   = "PART".getBytes();
    final static byte[] VISIBLE_COMMITTED = "CMMT".getBytes();
    final static byte[] VISIBLE_DONE      = "DONE".getBytes();

    
    private boolean _syncIO = false;
    
    private String _pathName;   //pathname for the prewrite image journal file
    private boolean _deleteOnClose;
    private int _pwjbSize;
    private int _pwjbCount;
    
    private PrewriteJournalBuffer[] _prewriteJournalBuffers;

    /**
     * The generation into which pages are currently being reserved.
     */    
    private long _currentGeneration = 1;
    /**
     * The last committed generation.
     */
    private long _committedGeneration = 0;
    
    private long _writerPollInterval = DEFAULT_WRITER_POLL_INTERVAL;
        
    private boolean _suspended;
    private boolean _stopped;
    
    private long _openTime;
    private long _closeTime;
    
    private long _reservationCount;
    private long _unreservationCount;
    

    
    PrewriteJournal(final Persistit persistit, final String pathName)
    throws PersistitException
    {
        this(persistit, pathName, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_COUNT, false, true);
    }
    
    PrewriteJournal(
    	final Persistit persistit,
        final String pathName,
        final int pwjbSize,
        final int pwjbCount,
        final boolean syncIO,
        final boolean deleteOnClose)
    throws PersistitException
    {
    	super(persistit);
        if (pwjbSize < MINIMUM_PWJB_SIZE ||
            pwjbSize > MAXIMUM_PWJB_SIZE)
        {
            throw new IllegalArgumentException(
                "Invalid prewrite journal buffer size: " + pwjbSize);
        }
        if (pwjbCount < 1 || pwjbCount > MAXIMUM_PWJB_COUNT)
        {
            throw new IllegalArgumentException(
                "Invalid prewrite journal buffer count: " + pwjbCount);
        }
        
        _syncIO = syncIO;
        _pathName = pathName;
        _deleteOnClose = deleteOnClose;
        try
        {
            RandomAccessFile raf = new RandomAccessFile(pathName, "rw");
            
            _pwjbSize = pwjbSize;
            _pwjbCount = pwjbCount;
            long totalSize = pwjbSize * pwjbCount;
            
            raf.setLength(totalSize);
            raf.close();
    
            _openTime = _persistit.elapsedTime();
                        
            _prewriteJournalBuffers = new PrewriteJournalBuffer[pwjbCount];
            
            for (int index = 0; index < pwjbCount; index++)
            {
                _prewriteJournalBuffers[index] =
                    new PrewriteJournalBuffer(pathName, index);
            }

            PrewriteJournalBuffer pwjb = getPWJB(_currentGeneration);
            pwjb.reset(_currentGeneration);
            
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_STARTED)) _persistit.getLogBase().log(LogBase.LOG_STARTED);
        }
        catch (FileNotFoundException fnfe)
        {
            throw new PrewriteJournalPathException(fnfe.getMessage());
        }
        catch (IOException ioe)
        {
            throw new PersistitIOException(ioe);
        }
    }
    
    /**
     * Reserves space in this prewrite journal buffer for the buffer.
     * Any process that is about to modify a page (whether already dirty or not)
     * must call this method first.  If this method returns <i>false</i>, then
     * the caller must relinquish claims on all resources it holds, sleep
     * briefly, and then retry.
     * <p>
     * Upon calling this method the caller must hold a writer claim on the
     * buffer.
     * @param buffer        the buffer that is about to be changed
     * @param buffer2       if non-null, a buffer that must occupy the
     *                      same generation as this one.
     * @throws RetryException if the PrewriteJournalBuffer is full so that
     * the reservation cannot be made
     */
    void reserve(Buffer buffer, Buffer buffer2)
    throws RetryException
    {
        //
        // Calling Thread must own a writer claim on the buffer.
        //
        if (Debug.ENABLED)
        {
            Debug.$assert(
                (buffer._status & Buffer.CLAIMED_MASK) > 0 &&
                buffer.isMine());
            if (Debug.isSuspended()) throw getRetryException();
        }

        if (_suspended || _stopped)
        {
            throw getRetryException();
        }
        if (buffer.getVolume().isTemporary()) return;
        long generation = buffer.getReservedGeneration();
        long generation2 = 0;
        if (buffer2 != null) generation2 = buffer2.getReservedGeneration();
        
        synchronized(_lock)
        {
            PrewriteJournalBuffer pwjb;
            //
            // If the baseline buffer is reserved in a prior generation
            // then we can't proceed.
            //
            if (generation2 != 0 && generation2 != _currentGeneration)
            {
                pwjb = getPWJB(generation2);
                if (Debug.ENABLED) Debug.$assert(
                    generation2 == pwjb._generation &&
                    generation2 < _currentGeneration);
                throw pwjb.getRetryException();
            }

            //
            // If the buffer is reserved in a prior generation then we now
            // are obligated to wait until that generation is committed
            // because we are prohibited from making changes in the buffer
            // that may comingle post-commit updates.
            //
            if (generation != 0 && generation != _currentGeneration)
            {
                pwjb = getPWJB(generation);
                if (Debug.ENABLED) Debug.$assert(
                    generation == pwjb._generation &&
                    generation < _currentGeneration);
                throw pwjb.getRetryException();
            }

            // We do not need to recheck the reserved generation field of the
            // buffer because this thread has an exclusive claim on the buffer.
            //
            // The buffer is currently unreserved in any generation.  We now
            // try to fit it into the current generation.
            //
            pwjb = getPWJB(_currentGeneration);
            //
            // If the buffer is reserved in a currently open generation, then
            // we don't need to re-reserve - just add more changes to this
            // generation.
            //
            if (generation == _currentGeneration &&
                pwjb.getState() == OPEN)
            {
                _reservationCount++;
                return;
            }
            // Resets the state of the current generation if appropriate.
            if ((pwjb.getState() == EMPTY) ||
                (pwjb.getState() == WRITTEN && pwjb._generation < _currentGeneration))
            {
                pwjb.reset(_currentGeneration);
            }
            
            if (pwjb.getState() != OPEN || !pwjb.reserveBuffer(buffer))
            {
                if (generation2 != 0)
                {
                    throw pwjb.getRetryException();
                }
                // if that failed, then try to advance to the next generation
                long nextGeneration = _currentGeneration + 1;
                PrewriteJournalBuffer pwjb2 = getPWJB(nextGeneration);
                if (pwjb2.getState() == EMPTY || pwjb2.getState() == WRITTEN)
                {
                    _currentGeneration = nextGeneration;
                    if (Debug.ENABLED) Debug.$assert(_currentGeneration - _committedGeneration <= _pwjbCount);
                    
                    pwjb2.reset(nextGeneration);
                    if (_persistit.getLogBase().isLoggable(LogBase.LOG_NEWGEN))
                    {
                        _persistit.getLogBase().log(LogBase.LOG_NEWGEN, _currentGeneration);
                    }
                    if (!pwjb2.reserveBuffer(buffer))
                    {
                        throw pwjb2.getRetryException();
                    }
                }
                else throw pwjb2.getRetryException();
            }
            _reservationCount++;
        }
    }
    
    void unreserve(Buffer buffer)
    {
        //
        // We can test whether true unreservation operation is required before
        // entering the synchronized block only because the caller is required
        // to have a writer claim on the buffer.
        //
        // Calling Thread must own a writer claim on the buffer.
        //
        if (Debug.ENABLED) Debug.$assert(
            (buffer._status & Buffer.CLAIMED_MASK) > 0 &&
            buffer.isMine());
        if (Debug.ENABLED) Debug.suspend();
        //
        if (buffer.isDirty() || buffer.getReservedGeneration() == 0) return;
        
        synchronized(_lock)
        {
            PrewriteJournalBuffer pwjb = getPWJB(buffer.getReservedGeneration());
            // Release the reserved space in the buffer.
            pwjb.unreserveBuffer(buffer);
            // Remove the reserved generation from this buffer.
            buffer.setReservedGeneration(0);
            _unreservationCount++;
        }
    }

    /**
     * Triggers a WriterThread to start writing because the
     * current buffer is full.
     */    
    void kick()
    {
        final PrewriteJournalBuffer pwjb;
        synchronized(_lock) {
            pwjb = getPWJB(_currentGeneration);
        }
        pwjb.kick();
    }
    
    /**
     * Suspend or resume update operations.
     * @param suspend   <i>true</i> to suspend updates to all volumes
     * @return          <i>true</i> iff updates were already suspended
     */
    boolean suspend(boolean suspend)
    {
        // synchronized only to enforce memory coherency
        synchronized(_lock)
        {
            boolean temp = _suspended;
            _suspended = suspend;
            return temp;
        }
    }
    
    /**
     * Close the PrewriteJournal.
     * @throws TimeoutException
     */
    void close()
    throws TimeoutException, PersistitIOException
    {
        close(DEFAULT_CLOSE_TIMEOUT);
    }
    
    /**
     * Close the PrewriteJournal.
     * @param   timeout Time in milliseconds to wait for successfully closing
     *          the PrewriteJournalBuffer before throwing a TimeoutException
     * @throws TimeoutException
     */
    void close(long timeout)
    throws TimeoutException, PersistitIOException
    {
        if (_persistit.getLogBase().isLoggable(LogBase.LOG_CLOSE1)) _persistit.getLogBase().log(LogBase.LOG_CLOSE1);
        long now = System.currentTimeMillis();
        
        long finalGeneration;
        synchronized(_lock)
        {
            _stopped = true;
            finalGeneration = _currentGeneration;
        }
        
        waitForCompletion(finalGeneration, timeout);

        for (int index = 0; index < _pwjbCount; index++)
        {
            _prewriteJournalBuffers[index].stop();
        }
        
        for (int index = 0; index < _pwjbCount; index++)
        {
            _prewriteJournalBuffers[index].waitForClose(
                now + timeout - System.currentTimeMillis());
        }
        
        if (_deleteOnClose)
        {
            File file = new File(_pathName);
            file.delete();
        }
        if (_persistit.getLogBase().isLoggable(LogBase.LOG_CLOSE2)) _persistit.getLogBase().log(LogBase.LOG_CLOSE2);
        _closeTime = now();
    }
    
    /**
     * Detemines if there are any dirty buffers
     * @return  <i>true</i> if there are no dirty buffers
     */
    boolean isQuiescent()
    {
        synchronized(_lock)
        {
            boolean quiet = true;
            
            for (int index = 0; quiet && index < _pwjbCount; index++)
            {
                quiet &= _prewriteJournalBuffers[index].getState() == EMPTY ||
                         _prewriteJournalBuffers[index].getState() == WRITTEN;
            }
            return quiet;
        }
    }
    
    /**
     * Waits up to DEFAULT_CLOSE_TIMEOUT seconds for all dirty buffers
     * for all volumes to be written.
     * @return  <i>false</i> if and only if this method returned early due
     *          to the Must Stop setting
     * @throws TimeoutException
     */
    boolean ensureWritten()
    throws TimeoutException
    {
        return ensureWritten(null, DEFAULT_CLOSE_TIMEOUT);
    }
    
    /**
     * Waits up to DEFAULT_CLOSE_TIMEOUT seconds for all dirty buffers
     * for a specified volume to be written.
     * @param   volume      The volume to wait for.  If <i>null</i> then wait
     *                      for all volumes to become fully written.
     * @return  <i>false</i> if and only if this method returned early due
     *          to the Must Stop setting
     * @throws TimeoutException
     */
    boolean ensureWritten(Volume volume)
    throws TimeoutException
    {
        return ensureWritten(volume, DEFAULT_CLOSE_TIMEOUT);
    }
    
    /**
     * Waits up to a specified number of milliseconds for all dirty buffers
     * for a specified volume to be written.
     * @param   timeout     The maximum number of milliseconds to wait
     * @param   volume      The volume to wait for.  If <i>null</i> then wait
     *                      for all volumes to become fully written.
     * @return  <i>false</i> if and only if this method returned early due
     *          to the Must Stop setting
     * @throws TimeoutException
     */
    boolean ensureWritten(Volume volume, long timeout)
    throws TimeoutException
    {
        if (_persistit.getLogBase().isLoggable(LogBase.LOG_ENSUREWRITTEN1)) _persistit.getLogBase().log(LogBase.LOG_ENSUREWRITTEN1);
        long expiration = now() + timeout;
        boolean waiting = true;
        long ensuredGeneration;
        long waitForGeneration = -1;
        synchronized(_lock)
        {
            ensuredGeneration = _currentGeneration;
        }
        
        while (waiting && now() < expiration)
        {
            boolean okay = true;
            PrewriteJournalBuffer waitForPWJB = null;
            synchronized(_lock)
            {
                for (long generation = ensuredGeneration;
                     okay &&
                     generation >= ensuredGeneration - _pwjbCount &&
                     generation > 0;
                     generation--)
                {
                    PrewriteJournalBuffer pwjb = getPWJB(generation);
                    if (pwjb._generation != generation) break;
                    if (pwjb.getState() != EMPTY && 
                        pwjb.getState() != WRITTEN &&
                        (volume == null || pwjb._volumes.contains(volume)))
                    {
                        waitForPWJB = pwjb;
                        waitForGeneration = waitForPWJB._generation;
                    } 
                }
                if (waitForPWJB == null)
                {
                    waiting = false;
                    break;
                }
            }
                
            waitForPWJB.kick();
            waitForCompletion(waitForGeneration, expiration - now());
        }
        if (_persistit.getLogBase().isLoggable(LogBase.LOG_ENSUREWRITTEN2)) _persistit.getLogBase().log(LogBase.LOG_ENSUREWRITTEN2);
        return true;
    }
    
    /**
     * Waits until the specified generation (and all previous generations)
     * have committed.
     * @param generation    The generation to wait for
     * @param timeout       The maximum time to wait before throwing
     *                      a TimeoutException
     * @return
     * @throws TimeoutException
     */
    private void waitForCommit(long generation, long timeout)
    throws TimeoutException
    {
        if (_persistit.getLogBase().isLoggable(LogBase.LOG_WAIT_PREV_GEN1))
        {
            _persistit.getLogBase().log(LogBase.LOG_WAIT_PREV_GEN1, generation);
        }
        WaitingThread wt = null;
        boolean committed = false;
        WaitingThreadManager wtm = null;
        synchronized(_lock)
        {
            if (_committedGeneration >= generation)
            {
                committed = true;
            }
            else
            {
                PrewriteJournalBuffer pwjb = getPWJB(generation);
                wtm = pwjb._wtmForCommit;
                wt = allocateWaitingThread();
                wt.setup(pwjb, false);
                wtm.enqueue(wt);
            }
        }
        
        if (!committed)
        {
            try
            {
                committed = wt.mediatedWait(timeout);
            }
            catch (InterruptedException ie)
            {
            }
            if (!committed)
            {
                synchronized(_lock)
                {
                    // Need to double-check this under synchronization
                    committed = wt.isNotified();
                    if (!committed) wtm.removeWaitingThread(wt);
                }
            }
            releaseWaitingThread(wt);
        }        
        if (Debug.ENABLED)
        {
            synchronized(_lock)
            {
                if (generation > 1)
                {
                    PrewriteJournalBuffer pwjb1 = getPWJB(generation);
                    PrewriteJournalBuffer pwjb0 = getPWJB(generation - 1);
                    if (pwjb0._generation == generation - 1 &&
                        pwjb1._generation == generation)
                    {
                        Debug.$assert(pwjb0.getState() > COPYING);
                    }
                }
            }
        }

        if (_persistit.getLogBase().isLoggable(LogBase.LOG_WAIT_PREV_GEN2))
        {
            _persistit.getLogBase().log(LogBase.LOG_WAIT_PREV_GEN2,
                generation, 0, 0, 0, 0,
                booleanSelector(committed), null, null, null, null);
        }
        
        if (!committed) throw new TimeoutException(
                "Generation " + generation +
                " not committed within " + timeout + " milliseconds");
                
    }
    
    boolean waitForCompletion(long generation, long timeout)
    {
        WaitingThread wt = null;
        boolean completed = false;
        WaitingThreadManager wtm = null;
        synchronized(_lock)
        {
            PrewriteJournalBuffer pwjb = getPWJB(generation);
            if (pwjb._generation > generation ||
                pwjb.getState() == WRITTEN)
            {
                completed = true;
            }
            else
            {   
                wtm = pwjb._wtmForCompletion;
                wt = allocateWaitingThread();
                wt.setup(pwjb, false);
                wtm.enqueue(wt);
            }
        }
        if (!completed)
        {
        	kick();
            try
            {
                completed = wt.mediatedWait(timeout);
            }
            catch (InterruptedException ie)
            {
            }
    
            if (!completed)
            {
                synchronized(_lock)
                {
                    completed = wt.isNotified();
                    {
                        if (!completed) wtm.removeWaitingThread(wt);
                    }
                }
            }
            releaseWaitingThread(wt);
        }
        return completed;
    }
    
    /**
     * Called by the PrewriteJournalBuffer's WriterThread to indicate that
     * the specified generation has been committed.
     * @param generation
     */
    private void committed(long generation)
    {
        if (Debug.ENABLED) Debug.$assert(_committedGeneration + 1 == generation);
        _committedGeneration++;
        PrewriteJournalBuffer pwjb = getPWJB(_committedGeneration);
        WaitingThreadManager wtm = pwjb._wtmForCommit;
        wtm.wakeAll();
    }
    
    /**
     * Once a generation has been closed then a writer thread works copies
     * all the dirty buffers from that generation into a prewrite journal
     * buffer and then commits that buffer.  As the writer thread copies each
     * buffer, it releases the reservation for that buffer and marks it clean,
     * making that buffer eligible for reuse by a different page.  For the
     * interval of time between copying the buffer to the prewrite journal
     * buffer and the time that page's content is actually written to the
     * Volume, the only valid copy of the page is in the prewrite journal buffer.
     * <p>
     * This method searches the uncommitted generations to find a transient copy
     * of the page.  If the a copy is found then this method copies it from
     * the prewrite journal buffer into the supplied buffer.  This method is
     * called by the {@link Volume#readPage} method before actually reading the
     * page from disk.
     * @param volume        The volume from which the page is to be read
     * @param pageAddr      Page address
     * @param buffer        Buffer into which page will be read
     * @return              <i>true</i> iff a valid copy of the page has been
     *                      recovered from the prewrite journal buffer.
     */
    boolean readFromPrewriteJournal(Volume volume, long pageAddr, Buffer buffer)
    {
        synchronized(_lock)
        {
            for (long generation = _currentGeneration;
                 generation > _currentGeneration - _pwjbCount &&
                 generation > 0; generation--)
            {
                PrewriteJournalBuffer pwjb = getPWJB(generation);
                if (pwjb.getState() == COPYING ||
                    pwjb.getState() == COMMITTING ||
                    pwjb.getState() == WRITING)
                {
                    if (pwjb.readBackReservation(volume, pageAddr, buffer))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }
   
    private PrewriteJournalBuffer getPWJB(long generation)
    {
        int index = ((int)(generation + _pwjbCount)) % _pwjbCount;
        return _prewriteJournalBuffers[index];
    }
    
    public String toString()
    {
        return displayStatus();
    }
    
    boolean isSyncIO()
    {
        return _syncIO;
    }
    
    private long now()
    {
        if (DEBUG_POPULATE_TIME_FIELDS)
        {
            return _persistit.elapsedTime();
        }
        else return System.currentTimeMillis();
    }
    
    /**
     * The generation number is an ever-increasing value that denotes
     * the sequencing of write operations to the prewrite journal buffer.
     * Any generation N will be committed to the prewrite journal before
     * generation N+1.  See {@link PrewriteJournal} for a detailed explanation
     * of journal sequencing
     * @return  The generation number
     */
    long getCurrentGeneration()
    {
        synchronized(_lock) {
            return _currentGeneration;
        }
    }
    
    static byte[] visibleIndex(int index)
    {
        if (index < 0 || index > MAXIMUM_PWJB_COUNT)
        {
            throw new IllegalArgumentException("index out of bounds" + index);
        }
        return Integer.toString(10000 + index).substring(1).getBytes();
    }
    
    void sync()
    throws PersistitIOException
    {
        for (int index = 0; index < _pwjbCount; index++)
        {
            PrewriteJournalBuffer pwj = _prewriteJournalBuffers[index];
            pwj.sync();
        }
    }
    
    int getPwjbCount()
    {
        return _pwjbCount;
    }
    
    int getPwjbSize()
    {
        return _pwjbSize;
    }
    
    //------------ Inner class represents one buffer ------------
    
    private static class Reservation
    {
        private Buffer _buffer;
        private long _volumeId;
        private long _page;
        private int _offset;
        private Thread _byThread;
        private long _atGeneration;
        private long _atTime;
        private boolean _hasLaterCommit;
        private long _copiedAt;
        private long _copyWait;
        private long _writtenAt;
        private long _writeWait;
        private int _blockedWriteCount; //DEBUG - debug
        private int _driveHash;
        
        private int compare(int driveHash, long volumeId, long page)
        {
            //
            // 20060521 - reversed the order so that writes are distributed
            // better to multiple volumes.
            //
            if (page < _page) return -1;
            if (page > _page) return 1;
            if (driveHash > _driveHash) return -1;
            if (driveHash < _driveHash) return 1;
            if (volumeId < _volumeId) return -1;
            if (volumeId > _volumeId) return 1;
            return 0;
        }
        
        private int compare(Reservation reservation)
        {
            return compare(reservation._driveHash, reservation._volumeId, reservation._page);
        }
        
        public String toString()
        {
            StringBuffer sb = new StringBuffer(Persistit.NEW_LINE);
            sb.append("Reservation for page=");
            sb.append(_page);
            sb.append(" by Thread=");
            sb.append(_byThread.getName());
            sb.append(" at generation=");
            sb.append(_atGeneration);
            if (_buffer == null) sb.append(" buffer=null");
            else if (_buffer.getPageAddress() == _page &&
                     _buffer.getVolume().getId() == _volumeId)
            {
                sb.append(" buffer status=");
                sb.append(_buffer.getStatusDisplayString());
            }
            else
            {
                sb.append(" buffer=(mismatched) ");
                sb.append(_buffer);
            }
            return sb.toString();
        }
    }
    
    
    private class PrewriteJournalBuffer
    {
        
        private RandomAccessFile _raf;
    
        private int _state;
        private int _nextAvail;
        private int _index;
        private int _volumeCount;
        private int _reservedBufferCount;
        private int _copiedBufferCount;
        private int _writtenBufferCount;
        
        private long _generation;
        private long _filePosition;
        private long _lastCommitTime;
        private long _lastWriteTime;
        
        private WriterThread _writerThread;
        
        private byte[] _visibleIndex;
        private byte[] _bytes;
        
        private final ArrayList _volumes = new ArrayList();
        
        private long _writingPage;
        private long _writingVolumeId;
        private boolean _stop = false;
        
        private Reservation[] _reservations;
    
        private RetryException _retryException;
        
        private WaitingThreadManager _wtmForCommit =
            new WaitingThreadManager(_persistit);
            
        private WaitingThreadManager _wtmForCompletion =
            new WaitingThreadManager(_persistit);
 
        private PrewriteJournalBuffer(String pathName, int index)
        throws IOException
        {
            _raf = new RandomAccessFile(pathName, "rw");
            _nextAvail = BUFFER_HEADER_SIZE;
            _bytes = new byte[_pwjbSize];
            _index = index;
            _filePosition = index * _pwjbSize;
            _visibleIndex = visibleIndex(index);

            writeStatusToBuffer(VISIBLE_UNUSED);
            if (INITIALIZE_TO_ZEROES)
            {
                for (int offset = 0;
                      offset < _pwjbSize;
                      offset += ZEROES.length)
                {
                    System.arraycopy(
                        ZEROES,
                        0,
                        _bytes,
                        offset,
                        Math.min(ZEROES.length, _pwjbSize - offset));
                }
                
                Debug.IOLogEvent iev = null;
                if (Debug.IOLOG_ENABLED)
                {
                    iev = Debug.startIOEvent("init pwj", _index);  //TODO
                }
                
                _raf.seek(_filePosition);
                _raf.write(_bytes);
                if (_syncIO) _raf.getFD().sync();
                
                if (Debug.IOLOG_ENABLED)
                {
                    Debug.endIOEvent(iev, null);
                }
                
            }
            _writerThread = new WriterThread(index);
            _writerThread.start();
            _reservations = new Reservation[_pwjbSize / Buffer.MIN_BUFFER_SIZE];
            _state = EMPTY;
        }
        
        private synchronized int getState() {
            return _state;
        }
        
        private synchronized void setState(final int state) {
            this._state = state;
        }
        
        private RetryException getRetryException() 
        {
            if (_retryException == null ||
                _retryException.getGeneration() != _generation ||
                DEBUG_ALWAYS_NEW_RETRY_EXCEPTION)
            {
                _retryException = new RetryException(_generation);
            }
            return _retryException;
        }
        
        private boolean readBackReservation(Volume volume, long page, Buffer buffer)
        {
            int slot = lookupReservation(volume, page);

            if (Debug.ENABLED) Debug.suspend();

            if (slot >= 0 && _reservations[slot]._buffer != null)
            {
                Reservation r = _reservations[slot];
                byte[] bytes = buffer.getBytes();
                if (!r._hasLaterCommit && r._offset != 0)
                {
                    System.arraycopy(_bytes, r._offset, bytes, 0, bytes.length);
                    return true;
                }
            }
            return false;
        }
        
        /**
         * Looks up a buffer in the _bufferArray.  The _bufferArray
         * is ordered by Volume ID and page address, and so we use a binary
         * search for good search performance.  The result is the index of
         * the matching array entry if it is found.  Otherwise the result is -N
         * where N is the index of the element immediately following the 
         * location where this Volume/pageAddr.  For example, when _bufferArray
         * is empty, this method will return -1.
         * @param volume
         * @param page
         * @return
         */
        private int lookupReservation(Volume volume, long page)
        {
            long id = volume.getId();
            int dh = volume.getDrive().hashCode();
            int slot1 = 0;
            int slot2 = _reservedBufferCount;
            return lookupReservation(dh, id, page, slot1, slot2);
        }
        
        private int lookupReservation(int dh, long id, long page, int slot1, int slot2)
        {    
            int slot = (slot1 + slot2) / 2;
            while (slot1 != slot2)
            {
                Reservation candidate = _reservations[slot];                
                int comparison = candidate.compare(dh, id, page);
                if (comparison == 0)
                {
                    return slot;
                }
                else if (comparison < 0)
                {
                    // The page we are seeking is below the current page.
                    slot2 = slot;
                    slot = (slot1 + slot2) / 2;
                }
                else
                {
                    slot1 = slot + 1;
                    slot = (slot1 + slot2) / 2;
                }
            }
            slot = -(slot + 1);
            return slot;
        }
        
        private void insertReservation(Buffer buffer)
        {
            int slot = lookupReservation(
                buffer.getVolume(), buffer.getPageAddress());
                
            if (slot >= 0)
            {
                if (Debug.ENABLED) Debug.$assert(
                    _reservations[slot]._buffer == buffer ||
                    _reservations[slot]._buffer == null);
            }
            else
            {
                slot = -slot - 1;
                Reservation reservation = _reservations[_reservedBufferCount];
                if (reservation == null) reservation = new Reservation();
                
                System.arraycopy(
                    _reservations,
                    slot,
                    _reservations,
                    slot + 1,
                    _reservedBufferCount - slot);
                    
                _reservations[slot] = reservation;
                
                reservation._buffer = buffer;
                reservation._volumeId = buffer.getVolume().getId();
                reservation._driveHash = buffer.getVolume().getDrive().hashCode();
                reservation._page = buffer.getPageAddress();
                reservation._offset = 0;
                reservation._hasLaterCommit = false;
                reservation._copiedAt = 0;
                reservation._writtenAt = 0;
                reservation._atGeneration = _generation;
                reservation._byThread = Thread.currentThread();
                if (DEBUG_POPULATE_TIME_FIELDS)
                {
                    reservation._atTime = now();
                }
                
                _reservedBufferCount++;
            }
        }
        
        private boolean removeReservation(Buffer buffer)
        {
            int slot = lookupReservation(
                buffer.getVolume(), buffer.getPageAddress());
            if (slot >= 0)
            {
                Reservation reservation = _reservations[slot];
                if (Debug.ENABLED) Debug.$assert(
                    reservation._buffer == buffer ||
                    reservation._buffer == null);
                if (Debug.ENABLED) Debug.$assert(
                    reservation._atGeneration ==
                    buffer.getReservedGeneration());
                
                _reservedBufferCount--;

                System.arraycopy(
                    _reservations,
                    slot + 1,
                    _reservations,
                    slot,
                    _reservedBufferCount - slot);
                
                reservation._buffer = null;
                reservation._volumeId = 0;
                reservation._driveHash = 0;
                reservation._page = 0;
                reservation._offset = 0;
                reservation._hasLaterCommit = false;
                reservation._copiedAt = 0;
                reservation._writtenAt = 0;
                reservation._atGeneration = 0;
                reservation._byThread = null;
                if (DEBUG_POPULATE_TIME_FIELDS)
                {
                    reservation._atTime = 0;
                }
                _reservations[_reservedBufferCount] = reservation;
                
                
                return true;
            }
            return false;
        }
        
        /**
         * Reserves space in this prewrite journal buffer for the buffer.
         * Any process that is about to dirty an otherwise clean page
         * must call this method first.
         * @param buffer        the buffer that is about to be changed
         * @return boolean      <i>true</i> iff there is room for the buffer
         */
        private boolean reserveBuffer(Buffer buffer)
        {
            Volume vol = buffer.getVolume();
            int size = buffer.getBytes().length + RECORD_HEADER_SIZE;
            boolean newVolume = !_volumes.contains(vol);
            if (newVolume) size += vol.metaDataLength();

            if (_nextAvail + size > _pwjbSize)
            {
                if (Debug.ENABLED) Debug.$assert(size <= (_pwjbSize - BUFFER_HEADER_SIZE));
                //
                // Casts this PrewriteJournalBuffer off, kicks its writer
                // thread, and returns false so that the caller will retry.
                //
                setState(CLOSED);
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_GEN_CLOSED)) _persistit.getLogBase().log(LogBase.LOG_GEN_CLOSED, _generation);

                kick();
                return false;
            }
            else
            {
                _nextAvail += size;
                insertReservation(buffer);                
                if (newVolume) _volumes.add(vol);
                
                buffer.setReservedGeneration(_generation);
                return true;
            }
        }
        
        /**
         * Release the reserved space held on behalf of this buffer.
         * @param status
         * @throws IOException
         */
        private boolean unreserveBuffer(Buffer buffer)
        {
            final int state = getState();
            if (_currentGeneration == _generation &&
                state == OPEN ||
                state == CLOSED)
            {
                if (removeReservation(buffer))
                {
                    int size = buffer.getBytes().length + RECORD_HEADER_SIZE;
                    if (Debug.ENABLED) Debug.$assert(_nextAvail - size >= BUFFER_HEADER_SIZE);
                    _nextAvail -= size;
                    return true;
                }
            }
            return false;
        }
    
        
        private void writeStatusToBuffer(byte[] status)
        throws IOException
        {
            if (Debug.ENABLED) Debug.$assert(status.length == 4);
            Util.putBytes(_bytes, HEADER_SIGNATURE, BUFFER_SIGNATURE);
            Util.putBytes(_bytes, HEADER_VISIBLE_INDEX, _visibleIndex);
            Util.putBytes(_bytes, HEADER_STATUS, status);
            Util.putInt(_bytes,  HEADER_PWJB_COUNT, _pwjbCount);
            Util.putInt(_bytes,  HEADER_NEXT_AVAIL, _nextAvail);
            Util.putInt(_bytes,  HEADER_SIZE, _pwjbSize);
            Util.putInt(_bytes,  HEADER_BUFFER_COUNT, _copiedBufferCount);
            Util.putInt(_bytes,  HEADER_VOLUME_COUNT, _volumeCount);
            Util.putLong(_bytes, HEADER_GENERATION, _generation);
            Util.putLong(_bytes, HEADER_OPEN_TIME, _openTime);
            Util.putLong(_bytes, HEADER_UPDATE_TIME, now());
            Util.putInt(_bytes,  HEADER_VERSION, Persistit.BUILD_ID);
            Util.putInt(_bytes, HEADER_UNUSED, 0);
        }
        
    
        /**
         * Harvests all the reserved buffers.  When done, all buffers that were
         * reserved are copied and are marked clean.
         * @throws InUseException
         * @throws IOException
         */
        private void copy()
        throws InUseException, IOException
        {

            // Regardless of whether there are any reserved pages, we are going
            // to log this generation so that the log is readable.
            //

            _copiedBufferCount = 0;
            _volumeCount = _volumes.size();
            writeStatusToBuffer(VISIBLE_PARTIAL);

            if (_persistit.getLogBase().isLoggable(LogBase.LOG_COPY1)) _persistit.getLogBase().log(LogBase.LOG_COPY1, _reservedBufferCount);
            
            // Copy the volume map to the journal buffer
            int next = BUFFER_HEADER_SIZE; 
            for (int index = 0; index < _volumeCount; index++)
            {
                Volume vol = (Volume)_volumes.get(index);
                next = vol.writeMetaData(_bytes, next);
            }
            
            // Copy the dirty buffers to the journal buffer
            for (int slot = 0; slot < _reservedBufferCount; slot++)
            {
                if (Debug.ENABLED) Debug.suspend();
                Reservation reservation = _reservations[slot];
                Buffer buffer = reservation._buffer;
                //
                // This can happen if we are repeating the attempt to
                // run copy() after an IOException. The buffer
                // may have been removed on the previous attempt because
                // it was clean, and therefore the reservation is no longer
                // valid.
                //
                if (buffer == null) continue;
                
                if (DEBUG_POPULATE_TIME_FIELDS)
                {
                    reservation._copiedAt = now();
                }
                // Get a non-exclusive claim on the buffer.
                // No thread should ever hold an exclusive claim on a buffer
                // for very long.
                //
                if (!buffer.claim(false))
                {
                    throw new InUseException(
                        "Thread " + Thread.currentThread().getName() +
                        " unable to get reader claim on " + buffer);
                }
                if (DEBUG_POPULATE_TIME_FIELDS)
                {
                    reservation._copyWait =
                        now() - reservation._copiedAt;
                }
                
                if (buffer.isDirty() &&
                    buffer.getReservedGeneration() == _generation &&
                    !buffer.isDeleted())
                {
                    if (Debug.ENABLED) Debug.$assert(
                        buffer.getPageAddress() == reservation._page &&
                        buffer.getVolume().getId() == reservation._volumeId);
                
                    if (buffer.getPageAddress() > 0)
                    {
                        try
                        {
                            buffer.clearSlack();
                        }
                        catch (InvalidPageStructureException ipse)
                        {
                            if (_persistit.getLogBase().isLoggable(LogBase.LOG_EXCEPTION))
                            {
                                _persistit.getLogBase().log(LogBase.LOG_EXCEPTION, ipse);
                            }
                        }
                    }
                    
                    byte[] bytes = buffer.getBytes();

                    buffer.save();
                    
                    Util.putBytes(_bytes, next, RECORD_SIGNATURE);
                    next += RECORD_SIGNATURE.length;
                    
                    Util.putInt(_bytes, next, bytes.length);
                    next += 4;
                        
                    Util.putLong(_bytes, next, buffer.getVolume().getId());
                    next += 8;
                        
                    Util.putLong(_bytes, next, buffer.getPageAddress());
                    next += 8;
                        
                    Util.putLong(_bytes, next, now());
                    next += 8;
                        
                    System.arraycopy(bytes, 0, _bytes, next, bytes.length);
                    
                    reservation._offset = next;
                    next += bytes.length;
                    _copiedBufferCount++;
                    
                    buffer.clean();                                                                                       
                    buffer.setReservedGeneration(0);
                }
                else
                {
                    if (buffer.isClean())
                    {
                        if (_persistit.getLogBase().isLoggable(LogBase.LOG_COPY_NOTDIRTY))
                        {
                            _persistit.getLogBase().log(LogBase.LOG_COPY_NOTDIRTY, buffer);
                        }
                        buffer.setReservedGeneration(0);
                    }
                    reservation._buffer = null;
                }
                buffer.release();
            }
            
            if (Debug.ENABLED) Debug.$assert(next <= _nextAvail);
            _nextAvail = next;
            Util.putInt(_bytes, 16, next);
            if (Debug.ENABLED) Debug.$assert(!_stopped);
        }
        
        private void commit()
        throws IOException, TimeoutException
        {
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_COPY2))
            {
                _persistit.getLogBase().log(LogBase.LOG_COPY3, _nextAvail, _copiedBufferCount);
            }
            
            Debug.IOLogEvent iev = null;
            if (Debug.IOLOG_ENABLED)
            {
                iev = Debug.startIOEvent("copy pwj", _generation);
            }

            if (_reservedBufferCount > 0)
            {
                //
                // do this write in 8MB chunks so that we don't have to 
                // allocate a humongous NIO buffer underneath.
                //
                _raf.seek(_filePosition);
                for (int from = 0; from < _nextAvail; from += MAX_JOURNAL_WRITE_SIZE)
                {
                    int size = Math.min(_nextAvail - from, MAX_JOURNAL_WRITE_SIZE);
                    _raf.write(_bytes, from, size);
                }
    
                if (Debug.IOLOG_ENABLED) Debug.endIOEvent(iev, null);
            }
            
            if (_syncIO) _raf.getFD().sync();

            if (_persistit.getLogBase().isLoggable(LogBase.LOG_COMMIT1)) _persistit.getLogBase().log(LogBase.LOG_COMMIT1);
            waitForCommit(_generation - 1, DEFAULT_CLOSE_TIMEOUT);
            writeStatusToBuffer(VISIBLE_COMMITTED);

            if (Debug.IOLOG_ENABLED)
            {
                iev = Debug.startIOEvent("commit pwj", _generation);
            }


            _raf.seek(_filePosition);
            _raf.write(_bytes, 0, BUFFER_HEADER_SIZE);

            if (_syncIO) _raf.getFD().sync();
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_COMMIT2)) _persistit.getLogBase().log(LogBase.LOG_COMMIT2);
            
            if (Debug.IOLOG_ENABLED)
            {
                Debug.endIOEvent(iev, null);
            }

        }
    
        private void write()
        throws IOException, PersistitException
        {
            //
            // At this point, the updates copied to the prewrite image buffer 
            // have been fully committed.  If the system were interrupted
            // here, we could recover from the journal file.
            //
            // Now work the array of buffers.
            //
            //
            int next = BUFFER_HEADER_SIZE;
            
            for (int index = 0; index < _volumeCount; index++)
            {
                next = Volume.confirmMetaData(_bytes, next);
            }
            
            for (int index = 0; index < _reservedBufferCount; index++)
            {

                Reservation reservation = _reservations[index];
                if (reservation._buffer == null || reservation._buffer.isDeleted()) continue;
                
                for (int i = 0; i < RECORD_SIGNATURE.length; i++)
                {
                    if (_bytes[next++] != RECORD_SIGNATURE[i])
                    {
                        throw new CorruptPrewriteJournalException(
                            "Invalid buffer record signature at next " +
                            (next - 1));
                    }
                }
                int bufferSize = Util.getInt(_bytes, next);
                next += 4;
                
                long id = Util.getLong(_bytes, next);
                next += 8;
                
                long page = Util.getLong(_bytes, next);
                next += 8;
                
//                long timeWritten = Util.getLong(_bytes, next);
                next += 8;

                Volume vol = _persistit.getVolume(id);
                if (vol == null)
                {
                    throw new CorruptPrewriteJournalException(
                        "Datatset for id=" + id + " not found in " + this);
                }
                if (bufferSize != vol.getBufferSize())
                {
                    throw new CorruptPrewriteJournalException(
                        "Invalid buffer size " + bufferSize + " at " + 
                        (next - RECORD_HEADER_SIZE) + 
                        " in " + this);
                }
                
                if (Debug.ENABLED) Debug.$assert(
                    reservation._page == page &&
                    reservation._volumeId == id);
                
                if (DEBUG_POPULATE_TIME_FIELDS)
                {
                    reservation._writtenAt = now();
                }

                // Prematurely halts the JVM to test recovery
                // Debug.debugExit(0.000005);
                
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_WRITEPAGE1)) _persistit.getLogBase().log(LogBase.LOG_WRITEPAGE1, page);
                
                writePage(vol, next, bufferSize, page, reservation);

                if (_persistit.getLogBase().isLoggable(LogBase.LOG_WRITEPAGE2)) _persistit.getLogBase().log(LogBase.LOG_WRITEPAGE2, page);
                if (DEBUG_POPULATE_TIME_FIELDS)
                {
                    reservation._writeWait = now() - reservation._writtenAt;
                }
                
                next += bufferSize;
            }
            
            // If we are syncio mode then sync each of the volumes we just
            // wrote to.
            //
            if (_syncIO)
            {

                for (int index = 0; index < _volumeCount; index++)
                {
                    Debug.IOLogEvent iev = null;
                    if (Debug.IOLOG_ENABLED)
                    {
                        iev = Debug.startIOEvent("sync volume", index);  //TODO
                    }
                    
                    Volume vol = (Volume)_volumes.get(index);
                    vol.sync();
                    
                    if (Debug.IOLOG_ENABLED)
                    {
                        Debug.endIOEvent(iev, null);    //TODO
                    }
                }
            }
            
            //
            // Now rewrite the header once more to indicate final completion.
            // This indicates that all the pages have been written to disk.
            //
            writeStatusToBuffer(VISIBLE_DONE);
            //
            // Flush it to disk
            //
            if (Debug.ENABLED) Debug.$assert(next == _nextAvail);
            
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_MARK1)) _persistit.getLogBase().log(LogBase.LOG_MARK1);
            
            Debug.IOLogEvent iev = null;
            if (Debug.IOLOG_ENABLED)
            {
                iev = Debug.startIOEvent("write done", _generation);  //TODO
            }
            try
            {
                _raf.seek(_filePosition);
                _raf.write(_bytes, 0, BUFFER_HEADER_SIZE);
                if (_syncIO) _raf.getFD().sync();
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_MARK2)) _persistit.getLogBase().log(LogBase.LOG_MARK2);
            }
            catch (IOException ioe)
            {
                if (Debug.IOLOG_ENABLED)
                {
                    Debug.endIOEvent(iev, ioe);
                }
                throw ioe;
            }
            if (Debug.IOLOG_ENABLED)
            {
                Debug.endIOEvent(iev, null);    //TODO
            }
        }
        
        private void writePage(
            Volume vol, 
            int next, 
            int bufferSize, 
            long page, 
            Reservation reservation)
        throws IOException, InvalidPageAddressException,
               ReadOnlyVolumeException, VolumeClosedException
        {
            boolean blocked = true;
            boolean hasLaterCommit = false;
            reservation._blockedWriteCount = 0;
            
            while (blocked)
            {
                if (Debug.ENABLED) Debug.suspend();

                blocked = false;
                long blockingGeneration =-1;
                synchronized(_lock)
                {
                    hasLaterCommit = reservation._hasLaterCommit;
                    if (hasLaterCommit) break;
                    
                    long limit = _generation - _pwjbCount;
                    if (limit < 0) limit = 0;
                    for (long gen = _generation - 1;
                         !blocked && gen >= limit; gen--)
                    {
                        PrewriteJournalBuffer pwjb = getPWJB(gen);
                        if (pwjb._generation != gen) break;
                        if (pwjb.getState() == WRITING &&
                            pwjb._writingVolumeId == vol.getId() &&
                            pwjb._writingPage == page)
                        {
                            blocked = true;
                            blockingGeneration = gen;
                        }
                    }
                    if (!blocked)
                    {
                        _writingVolumeId = vol.getId();
                        _writingPage = page;
                    }
                }
                if (blocked)
                {
                    if (_persistit.getLogBase().isLoggable(LogBase.LOG_WRITE_BLOCKED_PAGE))
                    {
                        _persistit.getLogBase().log(LogBase.LOG_WRITE_BLOCKED_PAGE, page, blockingGeneration);
                    }
                    reservation._blockedWriteCount++;
                    try
                    {
                        Thread.sleep(SHORT_POLL_INTERVAL);
                    }
                    catch (InterruptedException ie)
                    {
                    }
                } 
            }
            if (!hasLaterCommit)
            {

                if (Debug.ENABLED) Debug.suspend();

                try
                {
                    vol.writePage(_bytes, next, bufferSize, page);
                    if (Debug.HISTORY_ENABLED)
                    {
                        Debug.stateChanged(
                            vol,
                            page,
                            Util.getByte(_bytes, next + Buffer.TYPE_OFFSET),
                            "write",
                            Util.getLong(_bytes, next + Buffer.CHANGE_COUNT_OFFSET),
                            _generation);
                    }
                    _writtenBufferCount++;
                }
                catch (IOException e)
                {
                    _persistit.getLogBase().log(LogBase.LOG_EXCEPTION,
                        e + " while writing page " + page +" in volume " + vol);
                    throw e;
                }
                finally
                {
                    synchronized(_lock)
                    {
                        _writingVolumeId = -1;
                        _writingPage = -1;
                    }
                }
            }
            else
            {
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_LATER_COMMIT))
                {
                    {
                        _persistit.getLogBase().log(LogBase.LOG_LATER_COMMIT, page); 
                    }
                }
            }
        }
    
        
        private void markPriorCommits()
        {

            if (_reservedBufferCount == 0) return;
            
            for (long generation = _generation;
                 --generation > _generation - _pwjbCount &&
                 generation > 0; )
            {
                PrewriteJournalBuffer pwjb = getPWJB(generation);
                if (pwjb._generation != generation)
                {
                    break;
                }
                if (Debug.ENABLED) Debug.$assert (_committedGeneration >= pwjb._generation);
                // Note - the status may still be STATUS_COMMITTING because
                // the status change happens outside of synchronization.
                if ((pwjb.getState() == WRITING ||
                     pwjb.getState() == COMMITTING) &&
                    pwjb._reservedBufferCount > 0)
                {
                    int slot0 = 0;
                    int slot1 = 0;
                    Reservation r0 = pwjb._reservations[slot0];
                    Reservation r1 = _reservations[slot1];
                    for(;;)
                    {
                        if (Debug.ENABLED) Debug.$assert(r0 != null && r1 != null);
                        int comparison = r1.compare(r0);
                        if (comparison == 0)
                        {
                            // If this is true then there is a committed
                            // version of the page described by r1.
                            if (r1._buffer != null)
                            {
                                r0._hasLaterCommit = true;
                            }
                            slot0++;
                            slot1++;
                            if (slot0 >= pwjb._reservedBufferCount ||
                                slot1 >= _reservedBufferCount)
                            {
                                break;
                            }
                            r0 = pwjb._reservations[slot0];
                            r1 = _reservations[slot1];
                        }
                        else if (comparison < 0)
                        {
                            slot0 = pwjb.lookupReservation(
                                r1._driveHash,
                                r1._volumeId,
                                r1._page, 
                                slot0, 
                                pwjb._reservedBufferCount);
                            if (slot0 < 0) slot0 = -slot0 - 1;
                            if (slot0 >= pwjb._reservedBufferCount)
                            {
                                break;
                            }
                            r0 = pwjb._reservations[slot0];
                        }
                        else
                        {
                            slot1 = lookupReservation(
                                r0._driveHash,
                                r0._volumeId,
                                r0._page, 
                                slot1, 
                                _reservedBufferCount);
                            if (slot1 < 0) slot1 = -slot1 - 1;
                            if (slot1 >= _reservedBufferCount)
                            {
                                break;
                            }
                            r1 = _reservations[slot1];
                        }
                    }
                }
            }
        }
        
        private void closePWJ()
        throws IOException
        {
            if (_raf != null)
            {
                _raf.getFD().sync();
                _raf.close();
                _raf = null;
                setState(SHUTDOWN);
            }
        }
        
        private void stop()
        {
            _stop = true;
            kick();
            Thread wt = _writerThread;
            if (wt != null && wt.isAlive())
            {
                try
                {
                    wt.join(DEFAULT_CLOSE_TIMEOUT);
                }
                catch (InterruptedException ioe)
                {
                }
            }
        }
        
        private boolean waitForClose(long timeout)
        {
            Thread wt = _writerThread;
            if (timeout < SHORT_POLL_INTERVAL) timeout = SHORT_POLL_INTERVAL;
            if (wt != null && wt.isAlive())
            {
                try
                {
                    wt.join(timeout);
                }
                catch (InterruptedException ioe)
                {
                }
                return !wt.isAlive();
            }
            return true;
        }
        
        private void kick()
        {
            boolean notified;
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_KICK1)) _persistit.getLogBase().log(LogBase.LOG_KICK1, _writerThread.getName());

            synchronized(_writerThread)
            {
                if (!_writerThread._kicked)
                {
                    _writerThread._kicked = true;
                    _writerThread.notify();
                    notified = true;
                }
                else
                {
                    notified = false;
                }
            }
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_KICK2))
            {
                _persistit.getLogBase().log(LogBase.LOG_KICK2, _writerThread.getName(), booleanSelector(notified));
            } 

        }
    
        private void reset(long generation)
        {
            final int state = getState();
            if (Debug.ENABLED) Debug.$assert(state == EMPTY || state == WRITTEN);
            _generation = generation;
            _volumes.clear();
            _nextAvail = BUFFER_HEADER_SIZE;
            _reservedBufferCount = 0;
            _copiedBufferCount = 0;
            _writtenBufferCount = 0;
            _volumeCount = 0;
            _retryException = null;
            setState(OPEN);
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_RESET))
            {
                _persistit.getLogBase().log(LogBase.LOG_RESET, _generation, 0, 0, 0, 0,
                               displayStatus(), null, null, null, null);
            }
        }
        
        public synchronized String toString()
        {
            return "PWJ" + _index + " state=" + _state + 
                   " generation=" + _generation;
        }
        
        private class WriterThread
        extends Thread
        {
            boolean _kicked;
            boolean _frozen;

            private WriterThread(int index)
            {
                super("PWJ" + index);
            }
        
            /**
             * Overrides default getName() accessor so that log will show the
             * generation.
             * @return
             */
            String getWriterThreadName()
            {
                return "PWJ" + _index + ":" + getState() + ":" + _generation;
            }
        
            public final void run()
            {
                // This happens if Persistit initialization fails after the
                // PrewriteJournal is created.  We don't want this exception
                // to confuse the actual cause of failure.
                //
                //if (!_persistit.isInitialized()) return;
                for (;;)
                {
                    int errorRetryCount = 0;
                    try
                    {
                        boolean kicked;
                        boolean stop;
                        long now = now();
                        if (_persistit.getLogBase().isLoggable(LogBase.LOG_WAIT1)) {
                            _persistit.getLogBase().log(LogBase.LOG_WAIT1, _generation);
                        }
                        
                        synchronized(this)
                        {
                            if (errorRetryCount > 0)
                            {
                                long delay = ERROR_RETRY_INTERVAL * errorRetryCount;
                                if (delay > MAX_ERROR_RETRY_INTERVAL)
                                {
                                    delay = MAX_ERROR_RETRY_INTERVAL;
                                }
                                wait(delay);
                            }
                            else if (!_kicked)
                            {
                                wait(_writerPollInterval);
                            }
                            kicked = _kicked;
                            stop = _stop;
                            _kicked = false;
                            if (_frozen) continue;
                        }
                        
                        if (_persistit.getLogBase().isLoggable(LogBase.LOG_WAIT2))
                        {
                            _persistit.getLogBase().log(LogBase.LOG_WAIT2, 
                                getState(),
                                _generation,
                                now() - now,
                                0,
                                0,
                                booleanSelector(kicked),
                                booleanSelector(stop),
                                null,
                                null,
                                null); 
                        }
                        
                        if (stop)
                        {
                            // This is how the WriterThread terminates normally.
                            closePWJ();
                            break;
                        }
                        
                        boolean logGenerationClosed = false;
                        if (Debug.ENABLED) Debug.suspend();
                         
                        int state = PrewriteJournalBuffer.this.getState();
                        //
                        // If the state is WRITTEN it means this thread has
                        // already performed all processing required for
                        // this generation, meaning that we should just go
                        // back and wait some more.  If it is EMPTY then
                        // this generation hasn't even been reset yet,
                        // and so we just go back and wait.
                        //
                        if (state == WRITTEN || state == EMPTY) continue;
                        
                        //
                        // Even if the generation is empty we want to 
                        // close this generation so that we can
                        // commit it without mistakenly accepting
                        // new reservations.
                        //
                        if (state <= CLOSED)
                        {
                            logGenerationClosed = true;
                            setState(CLOSED);
                            state = CLOSED;
                        }
                        
                        if (logGenerationClosed && _persistit.getLogBase().isLoggable(LogBase.LOG_GEN_CLOSED))
                        {
                            _persistit.getLogBase().log(LogBase.LOG_GEN_CLOSED, _generation);
                        }
                        
                        if (Debug.ENABLED) Debug.suspend();
                        
                        if (state <= COPYING)
                        {
                            setState(COPYING);
                            state = COPYING;
                            long start = now();
                            copy();
                            logTime(start, now(), state);
                        }
                        
                        if (Debug.ENABLED) Debug.suspend();
                        
                        if (_persistit.getLogBase().isLoggable(LogBase.LOG_WAIT3))
                        {
                            _persistit.getLogBase().log(LogBase.LOG_WAIT3,
                                _state,
                                _generation,
                                now() - now,
                                _nextAvail,
                                0,
                                booleanSelector(kicked),
                                booleanSelector(_copiedBufferCount > 0),
                                null,
                                null,
                                null);
                        }
                        
                        // Commit this generation, whether there's anything to
                        // do or not.
                        //
                        if (state <= COMMITTING)
                        {
                            setState(COMMITTING);
                            state = COMMITTING;
                            long start = now();
                            commit();
                            logTime(start, now(), state);
                            _lastCommitTime = now();

                            if (_persistit.getLogBase().isLoggable(LogBase.LOG_COMMITTED))
                            {
                                _persistit.getLogBase().log(LogBase.LOG_COMMITTED,
                                    _generation, 0, 0, 0, 0,
                                    booleanSelector(_copiedBufferCount > 0), null, null, null, null);
                            }
                            
                            synchronized(_lock)
                            {
                                committed(_generation);
                                // Marks any Reservations in prior generations that are also
                                // writing to notify those prior generations that this page
                                // has been committed into a later generation.  Only this
                                // generation should write the page.
                                //
                                markPriorCommits();
                            }

                        }

                        if (Debug.ENABLED) Debug.suspend();

                        if (state <= WRITING)
                        {
                            setState(WRITING);
                            state = WRITING;
                            if (_persistit.getLogBase().isLoggable(LogBase.LOG_WRITE1))
                            {
                                _persistit.getLogBase().log(LogBase.LOG_WRITE1, _copiedBufferCount);
                            }
    
                            if (Debug.ENABLED) Debug.suspend();
                            
                            long start = now();
                            write();
                            logTime(start, now(), state);

                            _lastWriteTime = now();
                        }
                        
                        if (_persistit.getLogBase().isLoggable(LogBase.LOG_WRITE2))
                        {
                            _persistit.getLogBase().log(LogBase.LOG_WRITE2, _copiedBufferCount, _writtenBufferCount);
                        }

                        if (Debug.ENABLED) Debug.suspend();

                        setState(WRITTEN);
                        state = WRITTEN;

                        synchronized(_lock)
                        {
                            WaitingThreadManager wtm = _wtmForCompletion;
                            wtm.wakeAll();
                        }
                        
                        if (_persistit.getLogBase().isLoggable(LogBase.LOG_COMPLETED))
                        {
                            _persistit.getLogBase().log(LogBase.LOG_COMPLETED, _generation);
                        }
                        errorRetryCount = 0;
                        
                    }
                    catch (InterruptedException ie)
                    {
                        errorRetryCount++;
                        _persistit.getLogBase().log(LogBase.LOG_EXCEPTION,
                            ie + " in writer thread " + getName() +
                            " at generation " + _generation);
                    }
                    catch (PersistitException de)
                    {
                        errorRetryCount++;
                        _persistit.getLogBase().log(LogBase.LOG_EXCEPTION,
                            de + " in writer thread " + getName() +
                            " at generation " + _generation +
                            Persistit.NEW_LINE + LogBase.detailString(de));
                    }
                    catch (IOException ioe)
                    {
                        errorRetryCount++;
                        _persistit.getLogBase().log(LogBase.LOG_EXCEPTION,
                            ioe + " in writer thread " + getName() +
                            " at generation " + _generation +
                            Persistit.NEW_LINE + LogBase.detailString(ioe));
                    }
                }
            }
        }
        
        private void logTime(final long start, final long end, final int state) {
            final long elapsed = end - start;
            if (elapsed > 20000) {
                _persistit.getLogBase().log(mediatedThreadName(Thread.currentThread()) + 
                    "Long time " + elapsed + "ms in state " + STATE_NAMES[state] + 
                    ": " + displayStatus());
            }
        }
        
        private void sync()
        throws PersistitIOException
        {
            try
            {
                _raf.getFD().sync();
            }
            catch (IOException ioe)
            {
                throw new PersistitIOException(ioe);
            }
        }
        
        void populateInfo(ManagementImpl.PrewriteJournalBufferInfo info)
        {
            synchronized(_lock) {
                info._filePosition = _filePosition;
                info._generation = _generation;
                info._index = _index;
                info._nextAvail = _nextAvail;
                info._copiedBufferCount = _copiedBufferCount;
                info._reservedBufferCount = _reservedBufferCount;
                info._writtenBufferCount = _writtenBufferCount;
                if (_state == SHUTDOWN) info._state = "SHUTDOWN";
                else info._state = STATE_NAMES[_state];
                info._stop = _stop;
                info._time = now();
                info._timeSinceLastCommitted =
                    _lastCommitTime > 0 ? info._time - _lastCommitTime : 0;
                info._timeSinceLastWritten = 
                    _lastWriteTime > 0 ? info._time - _lastWriteTime : 0;
            }
        }
    }
    
    String displayStatus()
    {
        synchronized(_lock)
        {
            StringBuffer sb = new StringBuffer(Persistit.NEW_LINE + "        ");
            sb.append(" currentGeneration=");
            sb.append(_currentGeneration);
            sb.append(" committedGeneration=");
            sb.append(_committedGeneration);
            Util.fill(sb, Persistit.NEW_LINE + "        #", 1);
            Util.fill(sb, "generation", 12);
            Util.fill(sb, "state", 8);
            Util.fill(sb, "nextAvail", 10);
            Util.fill(sb, "reserved", 10);
            Util.fill(sb, "copied", 10);
            Util.fill(sb, "volcount", 10);
            sb.append(Persistit.NEW_LINE + "        ");
            for (int i = 0; i < _pwjbCount; i++)
            {
                PrewriteJournalBuffer pwjb = _prewriteJournalBuffers[i];
                Util.fill(sb, i, 1);
                Util.fill(sb, pwjb._generation, 12);
                Util.fill(sb, pwjb.getState(), 8);
                Util.fill(sb, pwjb._nextAvail, 10);
                Util.fill(sb, pwjb._reservedBufferCount, 10);
                Util.fill(sb, pwjb._copiedBufferCount, 10);
                Util.fill(sb, pwjb._volumeCount, 10);
                sb.append(Persistit.NEW_LINE + "        ");
            }
            sb.append(Persistit.NEW_LINE);
            return sb.toString();
        }
    }
    
    void populateInfo(ManagementImpl.PrewriteJournalInfo info)
    {
        info._committedGeneration = _committedGeneration;
        info._currentGeneration = _currentGeneration;
        info._deleteOnClose = _deleteOnClose;
        info._syncIO = _syncIO;
        info._open = _openTime != 0 && _closeTime == 0;
        info._openTime = _openTime;
        info._pathName = _pathName;
        info._pwjbCount = _pwjbCount;
        info._pwjbSize = _pwjbSize;
        info._writerPollInteval = _writerPollInterval;
        info._reservationCount = _reservationCount;
        info._unreservationCount = _unreservationCount;
        info._time = now();
    }

    void populateInfo(ManagementImpl.PrewriteJournalBufferInfo[] results)
    {
        for (int index = 0; index < _pwjbCount; index++)
        {
            _prewriteJournalBuffers[index].populateInfo(results[index]);
        }
    }
    
    void waitForReservation(RetryException re)
    {
        if (Debug.ENABLED)
        {
            //if (Debug.VERIFY_CLAIMS_ENABLED)  SharedResource.verifyNoClaims();
            Debug.suspend();
        }
        if (re == BufferPool.TV_RETRY_EXCEPTION)
        {
            try
            {
                Thread.sleep(SHORT_POLL_INTERVAL);
            }
            catch (InterruptedException ie)
            {
            }
        }
        else
        {
            waitForReservation(re, SHORT_DELAY_INTERVAL);
        }
    }
    
    /**
     * Wait up to time milliseconds for the generation specified by
     * the RetryException to become DONE.
     * @param re
     * @param time
     */
    void waitForReservation(RetryException re, long time)
    {
        waitForCompletion(re.getGeneration(), time);
    }
    
    public static String mediatedThreadName(Thread thread)
    {
        if (thread instanceof PrewriteJournalBuffer.WriterThread)
        {
            return ((PrewriteJournalBuffer.WriterThread)thread).getWriterThreadName();
        }
        else return thread.getName();
    }
    
    RetryException getRetryException()
    {
        synchronized(_lock) {
            return getPWJB(_currentGeneration).getRetryException();
        }
    }
    /**
     * Returns Boolean.TRUE or Boolean.FALSE.  Same as JDK1.4
     * Boolean.valueOf(boolean).  Needed because we want to compile under
     * JDK1.3.
     * @param b
     * @return
     */
    private static Boolean booleanSelector(boolean b)
    {
        return b ? Boolean.TRUE : Boolean.FALSE;
    }
}
