/*
 * Copyright (c) 2005 Persistit Corporation. All Rights Reserved.
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
 * Created on Mar 3, 2005
 */
package com.persistit;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.persistit.exception.PersistitException;
/**
 * <p>
 * Saves Persistit records to a DataOutputStream in the format
 * expected by a {@link StreamLoader} instance.
 * </p>
 * @version 1.0
 */
public class StreamSaver
extends Task
{
    /**
     * Record type marker for FILL records
     */
    public final static int RECORD_TYPE_FILL =         ('A' << 8) + 'Z';
    /**
     * Record type marker for DATA records
     */
    public final static int RECORD_TYPE_DATA =         ('D' << 8) + 'R';
    /**
     * Record type marker for KEY_FILTER records
     */
    public final static int RECORD_TYPE_KEY_FILTER =   ('K' << 8) + 'F';
    /**
     * Record type marker for VOLUME_ID records
     */
    public final static int RECORD_TYPE_VOLUME_ID =    ('V' << 8) + 'I';
    /**
     * Record type marker for TREE_ID records
     */
    public final static int RECORD_TYPE_TREE_ID =      ('T' << 8) + 'I';
    /**
     * Record type marker for HOSTNAME records
     */
    public final static int RECORD_TYPE_HOSTNAME =     ('H' << 8) + 'N';
    /**
     * Record type marker for USER records
     */
    public final static int RECORD_TYPE_USER =         ('H' << 8) + 'U';
    /**
     * Record type marker for COMMENT records
     */
    public final static int RECORD_TYPE_COMMENT =      ('C' << 8) + 'O';
    /**
     * Record type marker for FILL records
     */
    public final static int RECORD_TYPE_COUNT =        ('R' << 8) + 'C';
    /**
     * Record type marker for START records
     */
    public final static int RECORD_TYPE_START =        ('X' << 8) + 'S';
    /**
     * Record type marker for END records
     */
    public final static int RECORD_TYPE_END  =         ('X' << 8) + 'E';
    /**
     * Record type marker for TIMESTAMP records
     */
    public final static int RECORD_TYPE_TIMESTAMP =    ('T' << 8) + 'S';
    /**
     * Record type marker for EXCEPTION records
     */
    public final static int RECORD_TYPE_EXCEPTION =    ('E' << 8) + 'X';
    /**
     * Record type marker for COMPLETION records
     */
    public final static int RECORD_TYPE_COMPLETION =   ('Z' << 8) + 'Z';
    /**
     * Default count of records to written with elided keys.
     */
    public final static int DEFAULT_CYCLE_COUNT = 1024;
    /**
     * Size of the buffer for BufferedOutputStreams created by the
     * File constructors.
     */
    public final static int DEFAULT_BUFFER_SIZE = 65536;
    
    protected DataOutputStream _dos;
    
    protected Key _lastKey;
    protected Volume _lastVolume;
    protected Tree _lastTree;
    protected long _dataRecordCount = 0;
    protected long _otherRecordCount = 0;
    protected int _cycleCount = DEFAULT_CYCLE_COUNT;
    protected boolean _stop;
    protected Exception _lastException;
    protected boolean _verbose;
    protected int _recordCount;
    protected Tree[] _taskTrees;
    protected KeyFilter _taskKeyFilter;
    
    /**
     * Package-private constructor used by {@link ManagementImpl}
     * to instantiate a {@link Task}. The {@link #setupTask} method
     * must be called to specify trees and the output file name.
     *
     */
    StreamSaver(final Persistit persistit)
    {
    	super(persistit);
    	_lastKey = new Key(persistit);
    }
    /**
     * Construct a StreamSaver from the provided DataOutputStream. 
     * The DataOutputStream should be based on a BufferedOutputStream for
     * better performance. 
     * @param dos   The DataOutputStream
     */
    public StreamSaver(final Persistit persistit, final DataOutputStream dos)
    {
    	this(persistit);
        _dos = dos;
    }
    
    /**
     * Construct a StreamSaver from the provided File using a default buffer
     * size of 64K bytes.
     * @param file  The File to which data will be saved
     * @throws FileNotFoundException
     */
    public StreamSaver(final Persistit persistit, final File file)
    throws FileNotFoundException
    {
    	this(persistit);
        _dos = 
            new DataOutputStream(
                new BufferedOutputStream(
                    new FileOutputStream(file),
                    DEFAULT_BUFFER_SIZE));
    }
    
    /**
     * Construct a StreamSaver from the provided path name using a default
     * buffer size of 64K bytes.
     * @param pathName  The path name of the file to which data will be saved
     * @throws FileNotFoundException
     */
    public StreamSaver(final Persistit persistit, final String pathName)
    throws FileNotFoundException
    {
    	this(persistit);
        _dos = 
            new DataOutputStream(
                new BufferedOutputStream(
                    new FileOutputStream(pathName),
                    DEFAULT_BUFFER_SIZE));
    }
    
    
    /**
     * Construct a StreamSaver from the provided File using a specified buffer
     * size.
     * @param file          The File to which data will be saved
     * @param bufferSize    The buffer size
     * @throws FileNotFoundException
     */
    public StreamSaver(final Persistit persistit, final File file, int bufferSize)
    throws FileNotFoundException
    {
    	this(persistit);
        _dos = 
            new DataOutputStream(
                new BufferedOutputStream(
                    new FileOutputStream(file),
                    bufferSize));
    }
    
    /**
     * Construct a StreamSaver from the provided path name using a specified
     * buffer size.
     * @param pathName      The path name of the file to which data will be saved
     * @param bufferSize    The buffer size
     * @throws FileNotFoundException
     */
    public StreamSaver(final Persistit persistit, final String pathName, int bufferSize)
    throws FileNotFoundException
    {
    	this(persistit);
        _dos = 
            new DataOutputStream(
                new BufferedOutputStream(
                    new FileOutputStream(pathName),
                    bufferSize));
    }
    
    /**
     * Indicates whether verbose progress messages posted to the message log.
     * @return  <tt>true</tt> if progress comments are enabled, 
     * otherwise <tt>false</tt>.
     */
    public boolean isVerbose()
    {
        return _verbose;
    }
    
    /**
     * Enables or disables output of verbose progress messages to the
     * message logl
     * @param enabled   <tt>true</tt> to enable progress comments
     */
    public void setVerbose(boolean enabled)
    {
        _verbose = enabled;
    }
    
    /**
     * Returns the cycle count, which is the maximum number of records that
     * will be written with elided keys.  When this number of records
     * is reached, <tt>StreamSaver</tt> writes the next record with a full, 
     * unelided key value.  This permits easier visual inspection of the
     * save file.
     * @return      The cycle count
     */
    public int getCycleCount()
    {
        return _cycleCount;
    }
    
    /**
     * Sets the cycle count.  See {@link #getCycleCount} for a description of
     * this property.
     * @param count The cycle count
     */
    public void setCycleCount(int count)
    {
        _cycleCount = count;
    }
    
    /**
     * Closes this StreamSaver and the underlying DataOutputStream. If the
     * save operation ran to completion without error, this method writes a
     * COMPLETION record to the save file before closing it.  This record
     * indicates that the save file represents all the records requested by
     * the save operation.
     * 
     * @throws IOException
     */
    public void close()
    throws IOException
    {
        writeTimestamp();
        if (!_stop && _lastException == null) _dos.writeChar(RECORD_TYPE_COMPLETION);
        _lastTree = null;
        _lastVolume = null;
        _lastKey.clear();
        _dos.close();
    }
    
    /**
     * Writes the key/value pair represented by the current state of an 
     * <tt>Exchange</tt> into a DATA record. If the supplied <tt>Exchange</tt> 
     * is based on different volume or tree than the previously written record,
     * this method also writes VOLUME_ID and/or TREE_ID records before the
     * DATA record.  This allows the <tt>StreamLoader</tt> to apply the
     * data record to the correct volume and tree.
     * @param exchange      The <tt>Exchange</tt>
     * @throws IOException
     */
    protected void writeData(Exchange exchange)
    throws IOException
    {
        if (_lastVolume != exchange.getVolume())
        {
            writeVolumeInfo(exchange);
        }
        
        if (_lastTree != exchange.getTree())
        {
            writeTreeInfo(exchange);
        }
        
        writeData(exchange.getKey(), exchange.getValue());
        _recordCount++;
        if ((_recordCount % 100) == 0) poll();
    }
    
    
    /**
     * Writes a key/value pair  into a DATA record. 
     * @param key       The <tt>Key</tt>
     * @param value     The <tt>Value</tt>
     * @throws IOException
     */
    protected void writeData(Key key, Value value)
    throws IOException
    {
        int elisionCount = key.firstUniqueByteIndex(_lastKey);
        _dos.writeChar(RECORD_TYPE_DATA);
        _dos.writeShort(key.getEncodedSize());
        _dos.writeShort(elisionCount);
        _dos.writeInt(value.getEncodedSize());
        _dos.write(
            key.getEncodedBytes(), 
            elisionCount, 
            key.getEncodedSize() - elisionCount);
        _dos.write(
            value.getEncodedBytes(),
            0,
            value.getEncodedSize());
        
        _dataRecordCount++;
        key.copyTo(_lastKey);
        
        if (_cycleCount != 0 & (_dataRecordCount % _cycleCount) == 0)
        {
            writeRecordCount(_dataRecordCount, _otherRecordCount);
            _lastKey.clear();
        }
    }
    
    /**
     * Writes a RECORD_COUNT record.  This record conveys the counts of
     * data records and non-data records written to the stream so far, and
     * can be used to check the integrity of the save file.  The
     * RECORD_COUNT record is preceded by three FILL records to allow for
     * easier inspection of the save file.
     * @throws IOException
     */
    protected void writeRecordCount(long dataRecordCount, long otherRecordCount)
    throws IOException
    {
        _dos.writeChar(RECORD_TYPE_FILL);
        _dos.writeChar(RECORD_TYPE_FILL);
        _dos.writeChar(RECORD_TYPE_FILL);
        _dos.writeChar(RECORD_TYPE_COUNT);
        _dos.writeLong(dataRecordCount);
        _dos.writeLong(otherRecordCount);
        _otherRecordCount++;
    }
    
    /**
     * Writes a VOLUME_ID record for the <tt>Volume</tt> currently associated 
     * with the supplied <tt>Exchange</tt>.  The information is sufficient to
     * recreate a new, empty <tt>Volume</tt> having the same path name, 
     * original size and growth parameters as the <tt>Volume</tt> being saved.  
     * @param exchange      The <tt>Exchange</tt>
     * @throws IOException
     */
    protected void writeVolumeInfo(Exchange exchange)
    throws IOException
    {
        writeVolumeInfo(exchange.getVolume());
        _lastVolume = exchange.getVolume();
    }
    
    /**
     * Writes a TREE_ID record for the <tt>Tree</tt> currently associated with
     * the supplied <tt>Exchange</tt>.  The information is sufficient to
     * recreate a new, empty <tt>Tree</tt> having the same name as the 
     * <tt>Tree</tt> being saved.  
     * @param exchange
     * @throws IOException
     */
    protected void writeTreeInfo(Exchange exchange)
    throws IOException
    {
        writeTreeInfo(exchange.getTree());
        _lastTree = exchange.getTree();
    }
    
    /**
     * Writes a VOLUME_ID record for the supplied <tt>Volume</tt>.
     * The saved information is sufficient to recreate a new, empty 
     * <tt>Volume</tt> having the same path name, original size
     * and growth parameters as the <tt>Volume</tt> being saved.  
     * @param volume    The <tt>Volume</tt>
     * @throws IOException
     */
    protected void writeVolumeInfo(Volume volume)
    throws IOException
    {
        _dos.writeChar(RECORD_TYPE_VOLUME_ID);
        _dos.writeLong(volume.getId());
        _dos.writeLong(volume.getInitialpages());
        _dos.writeLong(volume.getExtensionPages());
        _dos.writeLong(volume.getMaximumPages());
        _dos.writeInt(volume.getBufferSize());
        _dos.writeUTF(volume.getPathName());
        _lastVolume = volume;
        _lastTree = null;
        _otherRecordCount++;
    }
    
    /**
     * Writes a TREE_ID record for the supplied <tt>Tree</tt>.  
     * The saved information is sufficient to recreate a new, 
     * empty <tt>Tree</tt> having the same name as the 
     * <tt>Tree</tt> being saved.  
     * @param tree      The <tt>Tree</tt>
     * @throws IOException
     */
    protected void writeTreeInfo(Tree tree)
    throws IOException
    {
        _dos.writeChar(RECORD_TYPE_TREE_ID);
        _dos.writeInt(tree.getTreeIndex());
        _dos.writeUTF(tree.getName());
        _lastTree = tree;
        _otherRecordCount++;
    }
    
    /**
     * Writes a TIMESTAMP record containing the current system time.
     * @throws IOException
     */
    protected void writeTimestamp()
    throws IOException
    {
        _dos.writeChar(RECORD_TYPE_TIMESTAMP);
        _dos.writeLong(System.currentTimeMillis());
    }
    
    /**
     * Writes a COMMENT record containing an arbitrary string
     * @param comment       The comment string
     * @throws IOException
     */
    protected void writeComment(String comment)
    throws IOException
    {
        _dos.writeChar(RECORD_TYPE_COMMENT);
        _dos.writeUTF("\r\n//" + comment + "//");
    }
    
    /**
     * Writes an EXCEPTION record, indicating an Exception that
     * occurred during the save process.
     * @param exception     The </tt>Exception</tt>
     * @throws IOException
     */
    protected void writeException(Exception exception)
    throws IOException
    {
        _dos.writeChar(RECORD_TYPE_EXCEPTION);
        _dos.writeUTF(exception.toString());
    }

    /**
     * Save all key/value pairs in the <tt>Tree</tt> associated with the
     * supplied <tt>Exchange</tt>, subject to selection by the supplied
     * <tt>KeyFilter</tt>. If the <tt>filter</tt> is <tt>null</tt> then save
     * all records. 
     * @param exchange      The <tt>Exchange</tt>
     * @param filter        The <tt>KeyFilter</tt>
     * @throws PersistitException
     * @throws IOException
     */
    public void save(Exchange exchange, KeyFilter filter)
    throws PersistitException, IOException
    {
        if (_verbose)
        {
            postMessage(
                "Saving Tree " + exchange.getTree().getName() + 
                " in volume " + exchange.getVolume().getPathName() + 
                (filter == null ? "" : " using KeyFilter: " + filter.toString()),
                LOG_VERBOSE);
        }

        writeTimestamp();
        _dos.writeChar(RECORD_TYPE_START);
        if (filter != null)
        {
            _dos.writeChar(RECORD_TYPE_KEY_FILTER);
            _dos.writeUTF(filter.toString());
        }
        Key key = exchange.getKey();
        key.clear().append(Key.BEFORE);
        while (exchange.traverse(Key.GT, filter, Integer.MAX_VALUE) & !_stop)
        {
            writeData(exchange);
        }
        writeRecordCount(_dataRecordCount, _otherRecordCount);
        _dos.writeChar(RECORD_TYPE_END);
        writeTimestamp();
    }
    
    /**
     * Saves one or more trees in a named volume. The volume is specified
     * by <tt>volumeName</tt>, which may either be the full path name of an
     * open volume, or a substring that uniquely matches one open volume.
     * @param volumeName    The volume name, or a substring that
     *                      matches only one volume.
     * @param selectedTreeNames
     *                      An array names of the trees to be saved.
     * @throws PersistitException
     * @throws IOException
     */
    public void saveTrees(String volumeName, String[] selectedTreeNames)
    throws PersistitException, IOException
    {
        Volume volume = _persistit.getVolume(volumeName);
        if (volume != null) saveTrees(volume, selectedTreeNames);
    }
    
    /**
     * Saves on or more trees in the specified <tt>Volume</tt>.
     * @param volume        The <tt>Volume</tt>
     * @param selectedTreeNames
     *                      An array names of the trees to be saved.
     * @throws PersistitException
     * @throws IOException
     */
    public void saveTrees(Volume volume, String[] selectedTreeNames)
    throws PersistitException, IOException
    {
        String[] treeNames = volume.getTreeNames();
        writeComment("Volume " + volume.getPathName());
        for (int index = 0; index < treeNames.length & !_stop; index++)
        {
            boolean selected = true;
            if (selectedTreeNames != null)
            {
                for (int index2 = 0; 
                     selected && index2 < selectedTreeNames.length; 
                     index2++)
                {
                    if (!selectedTreeNames[index2].equals(treeNames[index]))
                    {
                        selected = false;
                    }
                }
            }
            if (!selected) 
            {
                writeComment("Tree " + treeNames[index] + " not selected");
            }
            else
            {
                writeComment("Tree " + treeNames[index]);
                try
                {
                    Exchange exchange = _persistit.getExchange(volume, treeNames[index], false);
                    save(exchange, null);
                }
                catch (PersistitException exception)
                {
                    _lastException = exception;
                    writeException(exception);
                }
            }
        }
    }
    
    /**
     * Saves on or more trees.
     * @param trees     The <tt>Tree</tt>s to save
     * @throws PersistitException
     * @throws IOException
     */
    public void saveTrees(Tree[] trees, KeyFilter keyFilter)
    throws PersistitException, IOException
    {
        for (int index = 0; index < trees.length; index++)
        {
            Tree tree = trees[index];
            writeComment(
                "Tree " + tree.getName() + 
                " in " + tree.getVolume().getPathName());
            try
            {
                Exchange exchange = 
                    _persistit.getExchange(tree.getVolume(), tree.getName(), false);
                save(exchange, keyFilter);
            }
            catch (PersistitException exception)
            {
                _lastException = exception;
                writeException(exception);
            }
        }
    }
    
    /**
     * Save all trees in all open volumes.
     * @throws PersistitException
     * @throws IOException
     */
    public void saveAll()
    throws PersistitException, IOException
    {
        Volume[] volumes = _persistit.getVolumes();
        for (int index = 0; index < volumes.length & !_stop; index++)
        {
            saveTrees(volumes[index], null);
        }
    }
    
    protected void setupTask(String[] args)
    throws PersistitException, IOException
    {
        _taskTrees = parseTreeList(args[0]);
        if (args[1] != null && args[1].length() > 0)
        {
            _taskKeyFilter = new KeyFilter(args[1]);
        }
        _dos = 
            new DataOutputStream(
                new BufferedOutputStream(
                    new FileOutputStream(args[2]),
                    DEFAULT_BUFFER_SIZE));
    }
    
    protected void runTask()
    throws PersistitException, IOException
    {
        saveTrees(_taskTrees, _taskKeyFilter);
        close();
    }
    
    public String getStatus()
    {
        if (_lastTree == null)
        {
            return null;
        }
        else
        {
            return
                "Saving " +
                _lastTree.getName() + 
                " in " + _lastTree.getVolume().getPathName() + 
                " (" + _recordCount + ")";
        }
    }
}
