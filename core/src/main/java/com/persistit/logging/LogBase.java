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

package com.persistit.logging;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;

import com.persistit.logging.PersistitLogMessage.LogItem;

/**
 *
 */
public class LogBase {

    /**
     * Annotation for basic English log messages
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    @interface Message {
        String value();
    }

    @Message("INFO|%s")
    public final LogItem copyright = PersistitLogMessage.empty();

    @Message("INFO|Log started %tc")
    public final LogItem start = PersistitLogMessage.empty();

    @Message("INFO|Log ended %tc")
    public final LogItem end = PersistitLogMessage.empty();

    @Message("ERROR|%s")
    public final LogItem exception = PersistitLogMessage.empty();

    @Message("WARNING|Configuration parameter error %s}")
    public final LogItem configurationError = PersistitLogMessage.empty();

    @Message("INFO| Allocating %,d buffers of size %,d bytes")
    public final LogItem allocateBuffers = PersistitLogMessage.empty();

    @Message("INFO|Opening volume %s")
    public final LogItem openVolume = PersistitLogMessage.empty();

    @Message("ERROR|Journal write failure %s in %s")
    public final LogItem journalWriteError = PersistitLogMessage.empty();

    @Message("INFO|Recovery done: %,d recovered pages, %,d committed transactions, %,d errors")
    public final LogItem recoveryDone = PersistitLogMessage.empty();

    @Message("ERROR|Recovery failed due to %s - the journal needs to be repaired or discarded")
    public final LogItem recoveryFailure = PersistitLogMessage.empty();

    @Message("DEBUG|Valid recovery keystone file %s will be recovered to %,d")
    public final LogItem recoveryKeystone = PersistitLogMessage.empty();

    @Message("DEBUG|Valid recovery member file %s")
    public final LogItem recoveryValidFile = PersistitLogMessage.empty();

    @Message("DEBUG|Processing Recovery Record type %s at %s %s with timestamp %,d")
    public final LogItem recoveryRecord = PersistitLogMessage.empty();

    @Message("INFO|Recovery plan: will recover %,d pages and %,d committed transactions; will discard %,d uncommitted transactions")
    public final LogItem recoveryPlan = PersistitLogMessage.empty();

    @Message("INFO|Recovery progress: %,d committed transactions applied, %,d uncommitted transactions rolled back, %,d remaining ")
    public final LogItem recoveryProgress = PersistitLogMessage.empty();

    @Message("WARNING|Recovery exception %s at transaction %s")
    public final LogItem recoveryException = PersistitLogMessage.empty();

    @Message("INFO|%s proposed")
    public final LogItem checkpointProposed = PersistitLogMessage.empty();

    @Message("INFO|%s written at %,d")
    public final LogItem checkpointWritten = PersistitLogMessage.empty();

    @Message("INFO|%s recovered at %s")
    public final LogItem checkpointRecovered = PersistitLogMessage.empty();

    @Message("ERROR|%s while beginning transaction %s")
    public final LogItem txnBeginException = PersistitLogMessage.empty();

    @Message("ERROR|%s while committing transaction %s")
    public final LogItem txnCommitException = PersistitLogMessage.empty();

    @Message("ERROR|%s while ending transaction %s")
    public final LogItem txnEndException = PersistitLogMessage.empty();

    @Message("ERROR|%s while rolling back transaction %s")
    public final LogItem txnRollbackException = PersistitLogMessage.empty();

    @Message("INFO|Transaction neither committed nor rolled back %s")
    public final LogItem txnNotCommitted = PersistitLogMessage.empty();

    @Message("INFO|Starting AdminUI")
    public final LogItem startAdminUI = PersistitLogMessage.empty();

    @Message("INFO|MXBean %s is registered")
    public final LogItem mbeanRegistered = PersistitLogMessage.empty();

    @Message("INFO|MXBean %s is unregistered")
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

    @Message("TRACE|Read volume %s page %,d into buffer # %,d")
    public final LogItem readOk = PersistitLogMessage.empty();

    @Message("ERROR|IOException %s while reading volume %s page %,d into buffer # %,d")
    public final LogItem readException = PersistitLogMessage.empty();

    @Message("ERROR|Exception %s while writing volume %s page %,d")
    public final LogItem writeException = PersistitLogMessage.empty();

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

    @Message("INFO|Management RMI Server registered on %s")
    public final LogItem rmiServerRegistered = PersistitLogMessage.empty();

    @Message("INFO|Management RMI Server unregistered on %s")
    public final LogItem rmiServerUnregister = PersistitLogMessage.empty();

    @Message("WARNING|Exception while registering management RMI Server %s %s")
    public final LogItem rmiRegisterException = PersistitLogMessage.empty();

    @Message("WARNING|Exception while unregistering management RMI Server %s %s")
    public final LogItem rmiUnregisterException = PersistitLogMessage.empty();

    @Message("WARNING|Unindexed page %,d in volume=%s tree=%s - run IntegrityCheck to repair")
    public final LogItem unindexedPage = PersistitLogMessage.empty();

    @Message("INFO|Waited %,d seconds for IO operations to finish")
    public final LogItem waitForClose = PersistitLogMessage.empty();

    @Message("WARNING|Checkpoint %s written to journal before dirty page %d")
    public final LogItem lateWrite = PersistitLogMessage.empty();

    @Message("WARNING|%s has %,d stranded pages")
    public final LogItem strandedPages = PersistitLogMessage.empty();

    @Message("ERROR|BTree structure error %s")
    public final LogItem corruptVolume = PersistitLogMessage.empty();

    @Message("WARNING|Exception while flushing non-essential administrative data")
    public final LogItem adminFlushException = PersistitLogMessage.empty();

    public void configure(final PersistitLogger logger) {
        for (final Field field : this.getClass().getDeclaredFields()) {
            final Message annotation = field.getAnnotation(Message.class);
            if (annotation != null) {
                try {
                    final String[] v = annotation.value().split("\\|");
                    final PersistitLevel level = PersistitLevel.valueOf(v[0]);
                    final String message = v[1];
                    LogItem logItem = (LogItem) field.get(this);
                    logItem.configure(logger, level, message);
                } catch (Exception e) {
                    System.err.printf("%s on field %s with annotation %s", e, field, annotation);
                }
            }
        }
    }
}