/**
 * Copyright 2005-2012 Akiban Technologies, Inc.
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

package com.persistit.logging;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;

import com.persistit.logging.PersistitLogMessage.LogItem;

/**
 * <p>
 * Log message and configuration mechanism for Persistit's internal logging
 * method. This class contains a field for each log message template. Typical
 * use within the main body of code is this:
 * 
 * <pre>
 * <code>
 *     persistit.getLogBase().recoveryException.log(exception, location);
 * </code>
 * </pre>
 * 
 * where <code>recoveryException</code> is one of the fields of this class. Each
 * field contains a {@link LogItem} which is either enabled or disabled,
 * depending on the currently configured log level. An enabled
 * <code>LogItem</code> actually issues a log message, while a disabled
 * <code>LogItem</code> does nothing.
 * </p>
 */
public class LogBase {

    public final static String RECURRING = "%s (%,d similar occurrences in %,d seconds)";

    /**
     * Annotation for basic English log messages
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    @interface Message {
        String value();
    }

    @Message("INFO|Akiban Persistit %s")
    public final LogItem copyright = PersistitLogMessage.empty();

    @Message("DEBUG|Log started %tc")
    public final LogItem start = PersistitLogMessage.empty();

    @Message("DEBUG|Log ended %tc")
    public final LogItem end = PersistitLogMessage.empty();

    @Message("ERROR|%s")
    public final LogItem exception = PersistitLogMessage.empty();

    @Message("WARNING|Configuration parameter error %s}")
    public final LogItem configurationError = PersistitLogMessage.empty();

    @Message("INFO| Allocating %,d buffers of size %,d bytes")
    public final LogItem allocateBuffers = PersistitLogMessage.empty();

    @Message("INFO|Opening volume %s (%s)")
    public final LogItem openVolume = PersistitLogMessage.empty();

    @Message("ERROR|Journal write failure %s in %s at offset %,d")
    public final LogItem journalWriteError = PersistitLogMessage.empty();

    @Message("INFO|Recovery done: %,d retained pages, %,d committed transactions, %,d errors")
    public final LogItem recoveryDone = PersistitLogMessage.empty();

    @Message("ERROR|Recovery failed due to %s - the journal needs to be repaired or discarded")
    public final LogItem recoveryFailure = PersistitLogMessage.empty();

    @Message("DEBUG|Valid recovery keystone file %s will be recovered to %,d")
    public final LogItem recoveryKeystone = PersistitLogMessage.empty();

    @Message("DEBUG|Valid recovery member file %s")
    public final LogItem recoveryValidFile = PersistitLogMessage.empty();

    @Message("TRACE|Processing Recovery Record type %s at %s %s with timestamp %,d")
    public final LogItem recoveryRecord = PersistitLogMessage.empty();

    @Message("INFO|Recovery plan: will recover %,d pages and %,d committed transactions; will discard %,d uncommitted transactions")
    public final LogItem recoveryPlan = PersistitLogMessage.empty();

    @Message("INFO|Recovery progress: %,d committed transactions applied, %,d uncommitted transactions rolled back, %,d remaining ")
    public final LogItem recoveryProgress = PersistitLogMessage.empty();

    @Message("WARNING|Recovery exception %s at transaction %s")
    public final LogItem recoveryException = PersistitLogMessage.empty();

    @Message("DEBUG|%s proposed")
    public final LogItem checkpointProposed = PersistitLogMessage.empty();

    @Message("DEBUG|%s written at %,d")
    public final LogItem checkpointWritten = PersistitLogMessage.empty();

    @Message("DEBUG|%s recovered at %s")
    public final LogItem checkpointRecovered = PersistitLogMessage.empty();

    @Message("ERROR|%s while beginning transaction %s")
    public final LogItem txnBeginException = PersistitLogMessage.empty();

    @Message("ERROR|%s while committing transaction %s")
    public final LogItem txnCommitException = PersistitLogMessage.empty();

    @Message("ERROR|%s while ending transaction %s")
    public final LogItem txnEndException = PersistitLogMessage.empty();

    @Message("ERROR|%s while rolling back transaction %s")
    public final LogItem txnRollbackException = PersistitLogMessage.empty();

    @Message("WARNING|Transaction neither committed nor rolled back %s")
    public final LogItem txnNotCommitted = PersistitLogMessage.empty();

    @Message("WARNING|Transaction abandoned %s")
    public final LogItem txnAbandoned = PersistitLogMessage.empty();

    @Message("DEBUG|Starting AdminUI")
    public final LogItem startAdminUI = PersistitLogMessage.empty();

    @Message("DEBUG|MXBean %s is registered")
    public final LogItem mbeanRegistered = PersistitLogMessage.empty();

    @Message("DEBUG|MXBean %s is unregistered")
    public final LogItem mbeanUnregistered = PersistitLogMessage.empty();

    @Message("WARNING|Failure while registering MXBean: %s")
    public final LogItem mbeanException = PersistitLogMessage.empty();

    @Message("TRACE|Added new garbage chain leftPage=%,d, rightPage=%,d %s")
    public final LogItem newGarbageChain = PersistitLogMessage.empty();

    @Message("TRACE|No room in garbage page for new garbage chain leftPage=%,d, rightPage=%,d, garbageBuffer=%s")
    public final LogItem garbagePageFull = PersistitLogMessage.empty();

    @Message("TRACE|Created new garbageRoot page %s")
    public final LogItem newGarbageRoot = PersistitLogMessage.empty();

    @Message("TRACE|GarbagePage %,d is exhausted, will be allocated and new garbageRoot is %,d %s")
    public final LogItem garbagePageExhausted = PersistitLogMessage.empty();

    @Message("TRACE|Allocated allocPage=%,d from garbageChain %s")
    public final LogItem allocateFromGarbageChain = PersistitLogMessage.empty();

    @Message("TRACE|Removing garbage chain %s because rightPage=%,d == nextGarbagePage")
    public final LogItem garbageChainDone = PersistitLogMessage.empty();

    @Message("TRACE|Updating garbage chain %s new leftPage=%,d, rightPage=%,d")
    public final LogItem garbageChainUpdate = PersistitLogMessage.empty();

    @Message("ERROR|IOException %s while reading volume %s page %,d into buffer # %,d")
    public final LogItem readException = PersistitLogMessage.empty();

    @Message("ERROR|Exception %s while writing volume %s page %,d")
    public final LogItem writeException = PersistitLogMessage.empty();

    @Message("WARNING|Missing volume %s referenced at journal address %,d")
    public final LogItem missingVolume = PersistitLogMessage.empty();

    @Message("WARNING|Lost page %,d from missing volume %s referenced at journal address %,d")
    public final LogItem lostPageFromMissingVolume = PersistitLogMessage.empty();

    @Message("WARNING|Exception %s while copying volume %s page %,d from journal address %,d")
    public final LogItem copyException = PersistitLogMessage.empty();

    @Message("ERROR|IOException %s while creating temporary volume %s")
    public final LogItem tempVolumeCreateException = PersistitLogMessage.empty();

    @Message("DEBUG|Extending %s: old length=%,d, new length=%,d")
    public final LogItem extendNormal = PersistitLogMessage.empty();

    @Message("DEBUG|Volume %s is already larger: has length=%,d, proposed new length=%,d")
    public final LogItem extendLonger = PersistitLogMessage.empty();

    @Message("ERROR|IOException %s while extending %s: old length=%,d, new length=%,d")
    public final LogItem extendException = PersistitLogMessage.empty();

    @Message("DEBUG|Management RMI Server registered on %s")
    public final LogItem rmiServerRegistered = PersistitLogMessage.empty();

    @Message("DEBUG|Management RMI Server unregistered on %s")
    public final LogItem rmiServerUnregister = PersistitLogMessage.empty();

    @Message("WARNING|Exception while registering management RMI Server %s %s")
    public final LogItem rmiRegisterException = PersistitLogMessage.empty();

    @Message("WARNING|Exception while unregistering management RMI Server %s %s")
    public final LogItem rmiUnregisterException = PersistitLogMessage.empty();

    @Message("WARNING|Unindexed page %,d in volume=%s tree=%s - run IntegrityCheck to repair")
    public final LogItem unindexedPage = PersistitLogMessage.empty();

    @Message("INFO|Waited %,d seconds for IO operations to finish")
    public final LogItem waitForClose = PersistitLogMessage.empty();

    @Message("ERROR|Checkpoint %s written to journal before dirty page %d")
    public final LogItem lateWrite = PersistitLogMessage.empty();

    @Message("WARNING|%s has %,d stranded pages")
    public final LogItem strandedPages = PersistitLogMessage.empty();

    @Message("ERROR|BTree structure error %s")
    public final LogItem corruptVolume = PersistitLogMessage.empty();

    @Message("WARNING|%s while flushing non-essential administrative data")
    public final LogItem adminFlushException = PersistitLogMessage.empty();

    @Message("WARNING|%s while performing cleanup action %s")
    public final LogItem cleanupException = PersistitLogMessage.empty();

    @Message("WARNING|%s while pruning transaction record %s")
    public final LogItem pruneException = PersistitLogMessage.empty();

    @Message("WARNING|%s while pruning TimelyResource %s")
    public final LogItem timelyResourcePruneException = PersistitLogMessage.empty();

    @Message("WARNING|Transaction %s pruning incomplete at %s after rollback")
    public final LogItem pruningIncomplete = PersistitLogMessage.empty();

    @Message("WARNING|Crash retried %,d times on %s")
    public final LogItem crashRetry = PersistitLogMessage.empty();

    @Message("WARNING|Journal flush operation took %,dms last %,d cycles average is %,dms")
    public final LogItem longJournalIO = PersistitLogMessage.empty();

    @Message("INFO|Normal journal file count %,d")
    public final LogItem normalJournalFileCount = PersistitLogMessage.empty();

    @Message("WARNING|Too many journal files %,d")
    public final LogItem tooManyJournalFilesWarning = PersistitLogMessage.empty();

    @Message("ERROR|Too many journal files %,d")
    public final LogItem tooManyJournalFilesError = PersistitLogMessage.empty();

    @Message("INFO|Preloading buffer pool inventory recorded at %tc")
    public final LogItem bufferInventoryLoad = PersistitLogMessage.empty();

    @Message("INFO|Preloaded %,d of %,d buffers in %,d seconds")
    public final LogItem bufferInventoryProgress = PersistitLogMessage.empty();

    @Message("WARNING|Exception while writing buffer pool inventory %s")
    public final LogItem bufferInventoryException = PersistitLogMessage.empty();

    @Message("WARNING|Thread %s interrupted due to shutdown")
    public final LogItem interruptedAtClose = PersistitLogMessage.empty();

    public static String recurring(final String message, final int count, final long duration) {
        return String.format(RECURRING, message, count, duration);
    }

    public void configure(final PersistitLogger logger) {
        for (final Field field : this.getClass().getDeclaredFields()) {
            final Message annotation = field.getAnnotation(Message.class);
            if (annotation != null) {
                try {
                    final String[] v = annotation.value().split("\\|");
                    final PersistitLevel level = PersistitLevel.valueOf(v[0]);
                    final String message = v[1];
                    final LogItem logItem = (LogItem) field.get(this);
                    logItem.configure(logger, level, message);
                } catch (final Exception e) {
                    System.err.printf("%s on field %s with annotation %s", e, field, annotation);
                }
            }
        }
    }

}
