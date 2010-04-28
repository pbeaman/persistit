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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.Vector;

import com.persistit.exception.BufferUnavailableException;
import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.InUseException;
import com.persistit.exception.InvalidPageAddressException;
import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.RetryException;
import com.persistit.exception.TimeoutException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeClosedException;
import com.persistit.exception.VolumeFullException;

/**
 * Holds a collection of data organized logically into trees and physically
 * into blocks called pages.  All pages within a volume are the same size.  A
 * page must be 1024, 2048, 8192 or 16384 bytes in length. Page 0 is a
 * special control page for the volume.
 * <p>
 * A Volume can contain any number of {@link Tree}s. Each tree has a unique root
 * page.  Each Volume also contains one special directory tree that holds
 * information about all other trees.
 * <p>
 * Currently a Volume is hosted inside of a single file.  Future
 * implementations may permit a Volume to be split across multiple files,
 * allowing drive-spanning.
 */
public class Volume
extends SharedResource
{
    private final static String ATTR_ALIAS = "alias";
    private final static String ATTR_DRIVE = "drive";
    private final static String ATTR_CREATE = "create";
    private final static String ATTR_READONLY = "readOnly";
    private final static String ATTR_CREATEONLY = "createOnly";
    private final static String ATTR_TEMPORARY = "temporary";
    private final static String ATTR_PAGE_SIZE = "pageSize";
    private final static String ATTR_PAGE2_SIZE = "bufferSize";
    private final static String ATTR_ID = "id";
    
    private final static String ATTR_INITIAL_SIZE = "initialSize";
    private final static String ATTR_EXTENSION_SIZE = "extensionSize";
    private final static String ATTR_MAXIMUM_SIZE = "maximumSize";
    
    private final static String ATTR_INITIAL_PAGES = "initialPages";
    private final static String ATTR_EXTENSION_PAGES = "extensionPages";
    private final static String ATTR_MAXIMUM_PAGES = "maximumPages";
    /**
     * Designated Tree name for the special directory "tree of trees".
     */
    public final static String DIRECTORY_TREE_NAME = "_directory";
    /**
     * Key segment name for index by directory tree name.
     */
    public final static String BY_NAME = "byName";
    /**
     * Key segment name for index by directory tree index.
     */
    public final static String BY_INDEX = "byIndex";
    /**
     * Status information about this Volume. Must be 8 bytes long.
     * Intended to human-readable at the top of the file.
     * This status means that the database was shut down normally.
     */
    public final static byte[] STATUS_CLEAN = Util.stringToBytes("CLEAN \r\n");
    /**
     * Status information about this Volume. Must be 8 bytes long.
     * Intended to human-readable at the top of the file.
     * This status means that the database was not shut down normally, and
     * that there may be pending updates remaining in the prewrite journal.
     */
    public final static byte[] STATUS_DIRTY = Util.stringToBytes("DIRTY \r\n");
    
    /**
     * Signature value - human and machine readable confirmation that this
     * file resulted from Persistit.
     */
    public final static byte[] SIGNATURE = Util.stringToBytes("PERSISTI");

    /**
     * Volume identifier - human and machine readable confirmation that this
     * is a Persistit Volume.
     */
    public final static byte[] IDENTIFIER_SIGNATURE = Util.stringToBytes("VOL ");
        
    /**
     * Current product version number.
     */        
    public final static int VERSION = 100;
    /**
     * Minimum product version that can handle Volumes created by this version.  
     */
    private final static int MIN_SUPPORTED_VERSION = 100;
    /**
     * Minimum product version that can handle Volumes created by this version.  
     */
    private final static int MAX_SUPPORTED_VERSION = 199;
    
    /**
     * Count of RandomAccessFile objects to open for each Volume.  These are
     * shared among consumer threads.
     */
    private final static int PREALLOCATED_RAF_COUNT = 4;
    
    private final static int MAX_CHECKPOINT_RETRIES = 100;
    private final static int MAX_CREATE_TREE_RETRIES = 100;

    private final static int HEADER_SIZE = Buffer.MIN_BUFFER_SIZE;
    
    private final static boolean DEBUG_WRITE_LOG = false;
    
    private long _id;

    private String _pathName;
    private byte[] _pathNameBytes;
    private String _alias;
    private long _readCounter;
    private long _writeCounter;
    private long _getCounter;
    private long _openTime;
    private long _createTime;
    private long _lastReadTime;
    private long _lastWriteTime;
    private long _lastExtensionTime;
    private long _highestPageUsed;
    private long _pageCount;
    private long _initialPages;
    private long _extensionPages;
    private long _maximumPages;
    private long _firstAvailablePage;
    private long _directoryRootPage;
    private long _garbageRoot;
    private long _fetchCounter;
    private long _traverseCounter;
    private long _storeCounter;
    private long _removeCounter;
    
    private int  _bufferSize;
    
    private IOException _lastIOException;
    
    private Buffer _headBuffer;
    private BufferPool _pool;    
    private boolean _readOnly;
    private boolean _temporary;
    
    private PrewriteJournal _pwj;
    
    private Stack _rafStack = new Stack();
    private Stack _rafStackRW = new Stack();
    
    
    private PrintWriter printWriter;
    private long lastPageWritten;
    private long lastFlushTime;
    
    
    // String name --> Tree tree
    
    private HashMap<String, Tree> _treeNameHashMap = new HashMap<String, Tree>();
    
    // Tree index --> Tree tree
    private HashMap<Integer, Tree> _treeIndexHashMap = new HashMap<Integer, Tree>();
    
    private int _maxTreeIndex = 0;
    
    private boolean _closed;
    private Tree _directoryTree;
    
    private ArrayList<DeallocationChain> _deallocationList = new ArrayList<DeallocationChain>();
    
    private String _drive = "unknown";
    
    private static class DeallocationChain
    {
        int _treeIndex;
        long _leftPage;
        long _rightPage;
        
        DeallocationChain(int treeIndex, long leftPage, long rightPage)
        {
            _treeIndex = treeIndex;
            _leftPage = leftPage;
            _rightPage = rightPage;
        }
    }
    
    /**
     * Opens a Volume.  The volume must already exist.
     * @param pathName  The full pathname to the file containing the Volume.
     * @param ro        <tt>true</tt> if the Volume should be opened in read-
     *                  only mode so that no updates can be performed
     *                  against it.
     * @return          The Volume.
     * @throws PersistitException
     */
    static Volume openVolume(final Persistit persistit, final String pathName, final boolean ro)
    throws PersistitException
    {
        return openVolume(persistit, pathName, null, 0, ro);
    }
 
    /**
     * Opens a Volume with a confirming id.  If the id value is non-zero, then
     * it must match the id the volume being opened. 
     * 
     * @param pathName  The full pathname to the file containing the Volume.
     * 
     * @param alias     A  friendly name for this volume that may be used 
     *                  internally by applications. The alias need not be
     *                  related to the <tt>Volume</tt>'s pathname, and
     *                  typically will denote its function rather than
     *                  physical location. 
     * 
     * @param id        The internal Volume id value - if non-zero this 
     *                  value must match the id value stored in the Volume
     *                  header.
     *  
     * @param ro        <tt>true</tt> if the Volume should be opened in read-
     *                  only mode so that no updates can be performed
     *                  against it.
     *         
     * @return          The <tt>Volume</tt>.
     * 
     * @throws PersistitException
     */
    static Volume openVolume(
		final Persistit persistit, 
		final String pathName, 
		final String alias, 
		final long id, 
		final boolean ro)
    throws PersistitException
    {
        File file = new File(pathName);
        if (file.exists() && file.isFile())
        {
            return new Volume(persistit, pathName, alias, id, ro);
        }
        throw new PersistitIOException(new FileNotFoundException(pathName));
    }
    
    /**
     * Loads and/or creates a volume based on a String-valued
     * specification. The specification has the form:
     * <br />
     * <i>pathname</i>[,<i>options</i>]...
     * <br />
     * where options include:
     * <br />
     * <dl>
     * <dt><tt>alias</tt></dt>
     * <dd>An alias used in looking up the volume by name within Persistit 
     *      programs (see {@link com.persistit.Persistit#getVolume(String)}).
     *      If the alias attribute is not specified, the the Volume's path 
     *      name is used instead.
     * </dd>
     * <dt><tt>drive<tt></dt>
     * <dd>Name of the drive on which the volume is located. Sepcifying the
     *      drive on which each volume is physically located is optional. If
     *      supplied, Persistit uses the information to improve I/O throughput
     *      in multi-volume configurations by interleaving write operations
     *      to different physical drives.
     * </dd>
     * <dt><tt>readOnly</tt></dt>
     * <dd>Open in Read-Only mode. (Incompatible with create
     *      mode.)</dd>
     * 
     * <dt><tt>create</tt></dt>
     * <dd>Creates the volume if it does not exist. Requires
     *      <tt>bufferSize</tt>, <tt>initialPagesM</tt>, 
     *      <tt>extensionPages</tt> and <tt>maximumPages</tt> to be
     *      specified.</dd>
     * 
     * <dt><tt>createOnly</tt></dt>
     * <dd>Creates the volume, or throw a
     *      {@link VolumeAlreadyExistsException} if it already
     *      exists.</dd>
     * 
     * <dt><tt>temporary</tt></dt>
     * <dd>Creates the a new, empty volume regardless of whether an
     *      existing volume file already exists.</dd>
     * 
     * <dt><tt>id:<i>NNN</i></tt></dt>
     * <dd>Specifies an ID value for the volume. If the volume
     *      already exists, this ID value must match the ID that was
     *      previously assigned to the volume when it was created.
     *      If this volume is being newly created, this becomes its 
     *      ID number.</dd>
     * 
     * <dt><tt>bufferSize:<i>NNN</i></tt></dt>
     * <dd>Specifies <i>NNN</i> as the volume's buffer size 
     *      when creating a new volume.  <i>NNN</i> must be
     *      1024, 2048, 4096, 8192 or 16384</dd>.
     * 
     * <dt><tt>initialPages:<i>NNN</i></tt></dt>
     * <dd><i>NNN</i> is the initial number of pages
     *      to be allocated when this volume is first created.</dd>
     * 
     * <dt><tt>extensionPages:<i>NNN</i></tt></dt>
     * <dd><i>NNN</i> is the number of pages by which to
     *      extend the volume when more pages are required.</dd>
     * 
     * <dt><tt>maximumPages:<i>NNN</i></tt></dt>
     * <dd><i>NNN</i> is the maximum number of pages
     *      to which this volume can extend.</dd>
     *      
     * </dl>
     *    
     * 
     * @param volumeSpec        Volume specification
     * 
     * @return                  The <tt>Volume</tt>
     * 
     * @throws PersistitException
     */
    static Volume loadVolume(final Persistit persistit, final String volumeSpec)
    throws PersistitException
    {
        StringTokenizer mainTokenizer = new StringTokenizer(volumeSpec, ",");
        try
        {
            String dsPath = mainTokenizer.nextToken().trim();
            String alias = null;
            String drive = "";
            boolean readOnly = false;
            boolean create = false;
            boolean createOnly = false;
            boolean temporary = false;
            int bufferSize = 8192;
            long id = 0;
            long initialPages = -1;
            long extensionPages = -1;
            long maximumPages = -1;
            long initialSize = -1;
            long extensionSize = -1;
            long maximumSize = -1;
            
            while (mainTokenizer.hasMoreTokens())
            {
                String token = mainTokenizer.nextToken().trim();
                StringTokenizer innerTokenizer =
                    new StringTokenizer(token, ":");
                String attr = innerTokenizer.nextToken().trim();
                if (ATTR_READONLY.equals(attr)) readOnly = true;
                else if (ATTR_CREATE.equals(attr)) create = true;
                else if (ATTR_CREATEONLY.equals(attr)) createOnly = true;
                else if (ATTR_TEMPORARY.equals(attr)) temporary = true;
                else if (ATTR_ALIAS.equals(attr))
                {
                    String valueString = innerTokenizer.nextToken().trim();
                    if (valueString != null && valueString.length() > 0)
                    {
                        alias = valueString;
                    }
                }
                else if (ATTR_DRIVE.equals(attr))
                {
                    String valueString = innerTokenizer.nextToken().trim();
                    if (valueString != null && valueString.length() > 0)
                    {
                        drive = valueString;
                    }
                }
                else
                {
                    String valueString = innerTokenizer.nextToken().trim();
                    boolean bad = false;
                    long value =
                        Persistit.parseLongProperty(
                            attr, valueString, 0, Long.MAX_VALUE);
                            
                    if (ATTR_PAGE_SIZE.equals(attr) ||
                        ATTR_PAGE2_SIZE.equals(attr))
                    {
                        bufferSize = (value > Integer.MAX_VALUE)
                                     ? Integer.MAX_VALUE
                                     : (int)value; 
                    }
                    else if (ATTR_ID.equals(attr))
                    {
                        id = value;
                    }
                    else if (ATTR_INITIAL_PAGES.equals(attr))
                    {
                        initialPages = value;
                    }
                    else if (ATTR_EXTENSION_PAGES.equals(attr))
                    {
                        extensionPages = value;
                    }
                    else if (ATTR_MAXIMUM_PAGES.equals(attr))
                    {
                        maximumPages = value;
                    }
                    else if (ATTR_INITIAL_SIZE.equals(attr))
                    {
                        initialSize = value;
                    }
                    else if (ATTR_EXTENSION_SIZE.equals(attr))
                    {
                        extensionSize = value;
                    }
                    else if (ATTR_MAXIMUM_SIZE.equals(attr))
                    {
                        maximumSize = value;
                    }
                    else bad = true;
                    if (bad || innerTokenizer.hasMoreTokens())
                    {
                        throw new InvalidVolumeSpecificationException(
                            volumeSpec);  
                    }
                }
            }
            int n = 0;
            if (readOnly) n++;
            if (create) n++;
            if (createOnly) n++;
            if (temporary) n++;
            if (n > 1)
            {
                throw new InvalidVolumeSpecificationException(
                    volumeSpec +
                    ": readOnly, create, createOnly and temporary " +
                    "attributes are mutually exclusive");  
            }
            //
            // Allows size specification in bytes rather than pages.
            //
            if (bufferSize > 0)
            {
                if (initialPages == -1 && initialSize > 0)
                {
                    initialPages = (initialSize + (bufferSize - 1)) / bufferSize;
                }
                if (extensionPages == -1 && extensionSize > 0)
                {
                    extensionPages = (extensionSize + (bufferSize - 1)) / bufferSize;
                }
                if (maximumPages == -1 && maximumSize > 0)
                {
                    maximumPages = (maximumSize + (bufferSize - 1)) / bufferSize;
                }
            }
            Volume vol;
            if (create || createOnly || temporary)
            {
                vol = create(
                		persistit,
                        dsPath, 
                        alias,
                        id, 
                        bufferSize, 
                        initialPages, 
                        extensionPages, 
                        maximumPages, 
                        createOnly,
                        temporary);
            }
            else
            {
                vol = openVolume(persistit, dsPath, alias, id, readOnly);
            }
            return vol;
        }
        catch (NumberFormatException nfe)
        {
            throw new InvalidVolumeSpecificationException(
                volumeSpec + ": invalid number");
        }
        catch (NoSuchElementException nste)
        {
            throw new InvalidVolumeSpecificationException(
                volumeSpec + ": " + nste);
        }
    }

    /**
     * Produces a displayable form of the volume specification.
     *  
     * @param volDesc    The description
     * 
     * @return
     */
    static String describe(String volDesc)
    {
        int p = volDesc.indexOf(',');
        if (p < 0) return volDesc;
        return volDesc.substring(0, p);
    }

    /**
     * Utility method to determine whether a subarray of bytes in a byte-array
     * <code>source</code> matches the byte-array in <code>target</code>.
     * @param source    The source byte array
     * @param offset    The offset of the sub-array within the source.
     * @param target    The target byte array
     * @return
     */
    private boolean bytesEqual(byte[] source, int offset, byte[] target)
    {
        for (int index = 0; index < target.length; index++)
        {
            if (source[index + offset] != target[index]) return false;
        }
        return true;
    }

    /**
     * Opens an existing Volume.  Throws CorruptVolumeException if the
     * volume file does not exist, is too short, or is malformed.
     * @param pathName
     * @param alias
     * @param id
     * @param readOnly
     * @throws PersistitException
     */
    private Volume(final Persistit persistit, String pathName, String alias, long id, boolean readOnly)
    throws PersistitException
    {
    	super(persistit);
        try
        {
            _pwj = persistit.getPrewriteJournal();
            _pathName = pathName;
            _pathNameBytes = Util.stringToBytes(pathName);
            _alias = alias;
            _readOnly = readOnly;

            preallocateRafs();

            RandomAccessFile raf = getRaf();
            long length = raf.length();
            if (length < HEADER_SIZE)
            {
                throw new CorruptVolumeException(
                    "Volume file too short: " + length);
            }
            byte[] bytes = new byte[HEADER_SIZE];
            raf.seek(0);
            raf.readFully(bytes);
            
            if (bytesEqual(bytes, 0, STATUS_CLEAN))
            {
                clean();
            }
            else if (bytesEqual(bytes, 0, STATUS_DIRTY))
            {
                dirty();
            }
            else
            {
                throw new CorruptVolumeException("Invalid status");
            }
            
            if (!bytesEqual(bytes, 8, SIGNATURE))
            {
                throw new CorruptVolumeException("Invalid signature");
            }

            int version = Util.getInt(bytes, 16);
            if (version < MIN_SUPPORTED_VERSION ||
                version > MAX_SUPPORTED_VERSION)
            {
                throw new CorruptVolumeException(
                    "Unsupported version " + version + 
                    " (must be in range " + MIN_SUPPORTED_VERSION + 
                    " - " + MAX_SUPPORTED_VERSION + ")");
            }
            getHeaderInfo(bytes);
            if (id != 0 && id != _id)
            {
                throw new CorruptVolumeException(
                    "Attempt to open with invalid id " + id +
                    " (!= " + _id + ")" );
            }
            long expectedLength = _pageCount * _bufferSize;
            if (length < expectedLength)
            {
                throw new CorruptVolumeException(
                    "Volume file too short: " + length +
                    " expected: " + expectedLength + " bytes");
            }
            _pool = _persistit.getBufferPool(_bufferSize);
            if (_pool == null)
            {
                throw new BufferUnavailableException(
                    "size: " + _bufferSize);
            }

            _headBuffer = _pool.get(this, 0, true, true);
            
            // TODO -- synchronize opening of Volumes.
            _persistit.addVolume(this);
            
            _directoryTree =
                new Tree(_persistit, this, DIRECTORY_TREE_NAME, 0, _directoryRootPage);
            
            if (Debug.ENABLED)
            {
                if (_garbageRoot > 0)
                {
                    Buffer garbageBuffer = null;
                    try
                    {
                        garbageBuffer = _pool.get(this, _garbageRoot, false, true);
                        if (Debug.ENABLED) Debug.$assert(garbageBuffer.isGarbagePage());
                    }
                    finally
                    {
                        if (garbageBuffer != null)
                        {
                            _pool.release(garbageBuffer);
                            garbageBuffer = null;
                        }
                    }
                }
            }
            
            _headBuffer.setPermanent(true);
            releaseHeadBuffer();
            
            releaseRaf(raf);
        }
        catch (IOException ioe)
        {
            throw new PersistitIOException(ioe);
        }
    }
    
    static Volume create(
    	final Persistit persistit,
    	final String pathName, 
    	final long id,
    	final int bufferSize,
    	final long initialPages,
    	final long extensionPages,
    	final long maximumPages,
    	final boolean mustCreate,
    	final boolean temporary)
    throws PersistitException
    {
        return create(persistit, pathName, null, id, bufferSize, initialPages, extensionPages, maximumPages, mustCreate, temporary);
    }
    /**
     * Creates a new Volume or open an existing Volume. If a volume having
     * the specifed <tt>id</tt> and <tt>pathname</tt> already exists, 
     * and if the mustCreate parameter is false, then this method opens and 
     * returns a previously existing <tt>Volume</tt>.  If <tt>mustCreate</tt> 
     * is true and a file of the specified name already exists, then this 
     * method throws a <tt>VolumeAlreadyExistsException</tt>.  Otherwise 
     * this method creates a new empty volume.
     * 
     * @param pathName      The full pathname to the file containing the Volume.
     * 
     * @param alias         A  friendly name for this volume that may be used 
     *                      internally by applications. The alias need not be
     *                      related to the <tt>Volume</tt>'s pathname, and
     *                      typically will denote its function rather than
     *                      physical location. 
     * 
     * @param id            The internal Volume id value -- if this value must
     *                      match the id value stored in the Volume header.
     *  
     * @param bufferSize    The buffer size (one of 1024, 2048, 4096, 8192 or
     *                      16384).
     * 
     * @param initialPages  Initialize number of pages to allocate.
     *  
     * @param extensionPages Number of additional pages to allocate when
     *                      all existing pages have been used.
     * 
     * @param maximumPages   A hard upper bound on the number of pages that can
     *                      be created within this volume.
     * 
     * @param mustCreate    <tt>true</tt> ensure that there is previously no
     *                      matching Volume, and that the Volume returned by
     *                      this method is newly created.
     * @param temporary     <tt>true</tt> to specify that this is a temporary
     *                      volume that should be reinitialized in an empty
     *                      state when opened.
     * @return              the Volume
     * @throws PersistitException
     */
    static Volume create(
    	final Persistit persistit,
    	final String pathName,
    	final String alias,
    	final long id,
    	final int bufferSize,
    	final long initialPages,
    	final long extensionPages,
    	final long maximumPages,
    	final boolean mustCreate,
    	final boolean temporary)
    throws PersistitException
    {
        File file = new File(pathName);
        if (file.exists() && file.length() >= HEADER_SIZE && !temporary)
        {
            if (mustCreate)
            {
                throw new VolumeAlreadyExistsException(pathName);
            }
            Volume vol = openVolume(persistit, pathName, alias, 0, false);
            if (vol._bufferSize != bufferSize)
            {
                throw new VolumeAlreadyExistsException(
                    "Different buffersize: " + vol);
            }
            //
            // Here we overwrite the former growth parameters
            // with those supplied to the create method.
            //
            vol._initialPages = initialPages;
            vol._extensionPages = extensionPages;
            vol._maximumPages = maximumPages;
            //
            // And set the alias
            //
            vol._alias = alias;
            return vol;
        }
        
        return new Volume(
            persistit,
            pathName, 
            alias,
            id,
            bufferSize,
            initialPages,
            extensionPages,
            maximumPages,
            temporary);
    }
    
    private Volume(
        final Persistit persistit,
        final String pathName,
        final String alias,
        final long id,
        final int bufferSize,
        long initialPages,
        long extensionPages,
        long maximumPages,
        final boolean temporary)
    throws PersistitException
    {
    	super(persistit);
        _pwj = _persistit.getPrewriteJournal();
        _pool = _persistit.getBufferPool(bufferSize);
        
        boolean sizeCheck = false;
        for (int b = Buffer.MIN_BUFFER_SIZE;
             !sizeCheck && b <= Buffer.MAX_BUFFER_SIZE; 	
             b *= 2)
        {
            if (bufferSize == b) sizeCheck = true;
        }
        
        if (!sizeCheck)
        {
            throw new InvalidVolumeSpecificationException(
                "Invalid buffer size: " + bufferSize);
        }
        if (_pool == null)
        {
            throw new BufferUnavailableException(
                "size: " + bufferSize);
        }

        if (initialPages == 0) initialPages = 1;
        if (maximumPages == 0) maximumPages = initialPages;
        
        if (initialPages < 0 || initialPages > Long.MAX_VALUE / bufferSize)
        {
            throw new InvalidVolumeSpecificationException(
                "Invalid initial page count: " + initialPages);
        }
            
        if (extensionPages < 0 || extensionPages > Long.MAX_VALUE / bufferSize)
        {
            throw new InvalidVolumeSpecificationException(
                "Invalid extension page count: " + extensionPages);
        }
            
        if (maximumPages < initialPages || maximumPages > Long.MAX_VALUE / bufferSize)
        {
            throw new InvalidVolumeSpecificationException(
                "Invalid maximum page count: " + maximumPages);
        }
        
        _pathName = pathName;
        _pathNameBytes = Util.stringToBytes(pathName);
        _alias = alias;
        _readOnly = false;
        
        try
        {
            preallocateRafs();
        }
        catch (IOException ioe)
        {
            throw new PersistitIOException(ioe);
        }
        if (temporary)
        {
            new File(_pathName).deleteOnExit();
            _pool.prepareTemporaryVolumeBufferWriters();
        }
        
        _bufferSize = bufferSize;

        long now = System.currentTimeMillis();
        if (id == 0)
        {
            _id = (now ^(((long)pathName.hashCode()) << 32)) & Long.MAX_VALUE;
        }
        else _id = id;
        _highestPageUsed = 0;
        _createTime = now;
        _lastExtensionTime = 0;
        _lastReadTime = 0;
        _lastWriteTime = 0;
        _firstAvailablePage = 1;
        _garbageRoot = 0;
        _pageCount = 1;
        _persistit.addVolume(this);        
        _headBuffer = _pool.get(this, 0, true, true);
        _headBuffer.setPermanent(true);
        _temporary = temporary;
        boolean fullyOpen = false;
        try
        {
            _headBuffer.clear();

            _initialPages = initialPages;
            _extensionPages = extensionPages;
            _maximumPages = maximumPages;
            //
            // Note: as a side-effect, this checkpoints all of the above into the
            // Volume file.
            //
            initialize(initialPages);
            dirty();
            fullyOpen = true;
        }
        finally
        {
            if (!fullyOpen) _headBuffer.setPermanent(false);
            releaseHeadBuffer();
        }
    }
    
    private void preallocateRafs()
    throws PersistitException, IOException
    {
        RandomAccessFile[] rafs = new RandomAccessFile[PREALLOCATED_RAF_COUNT];

        if (!_readOnly)
        {
            rafs[0] = getRafRW();
            releaseRafRW(rafs[0]);
        }
        
        for (int index = 0; index < rafs.length; index++)
        {
            rafs[index] = getRaf();
        }
        for (int index = rafs.length; --index >= 0; )
        {
            releaseRaf(rafs[index]);
        }
    }
    
    
    /**
     * Reinitializes the volume, removing all existing trees.
     */
    private void initialize(long initialPages)
    throws PersistitException
    {
        if (isReadOnly())
        {
            throw new ReadOnlyVolumeException(toString());
        }
        claimHeadBuffer(true);
        boolean reserved = false;
                
        try
        {
            for (int retries = MAX_CREATE_TREE_RETRIES;
                 --retries >= 0 && !reserved;)
            {
                try
                {
                    _pwj.reserve(_headBuffer, null);
                    reserved = true;
                }
                catch (RetryException re)
                {
                    _persistit.getPrewriteJournal().waitForReservation(re);
                }
            }
        
            if (!reserved)
            {
                throw new BufferUnavailableException(
                    "Can't reserve head buffer for " + this);
            }
            
            RandomAccessFile raf = null;
            long newLength = initialPages * _bufferSize;
            try
            {
                raf = getRafRW();
                raf.setLength(newLength);
            }
            catch (IOException ioe)
            {
                throw new PersistitIOException(ioe);
            }
            finally
            {
                if (raf != null) releaseRaf(raf);
            }
            _pageCount = initialPages;
            _firstAvailablePage = 1;
            _highestPageUsed = 0;
            _garbageRoot = 0;
            _directoryRootPage = 0;
            _pool.invalidate(this);
            
            Tree tree = new Tree(_persistit, this);
            _directoryTree = tree;
            
            boolean done = false;
            for (int retries = MAX_CREATE_TREE_RETRIES;
                 --retries >= 0 && !done;)
            {
                try
                {
                    createTree(DIRECTORY_TREE_NAME, tree);
                    done = true;
                }
                catch (RetryException rex)
                {
                    _persistit.getPrewriteJournal().waitForReservation(rex);
                }
            }
            if (!done)
            {
                _directoryTree = null;
                throw new BufferUnavailableException(
                    "could not create directory tree");
            }
            _directoryRootPage = tree.getRootPageAddr();
            checkpointMetaData();
        }
        finally
        {
            if (reserved && _headBuffer.isClean()) _pwj.unreserve(_headBuffer);
            releaseHeadBuffer();
        }
    }
    
    /**
     * Updates head page information.  The head buffer must be reserved
     * on entry.
     * @throws PMapException
     */
    private void checkpointMetaData()
    throws ReadOnlyVolumeException
    {
        if (_readOnly) throw new ReadOnlyVolumeException(toString());
        putHeaderInfo(_headBuffer.getBytes());
        _headBuffer.dirty();
    }

    /**
     * Returns the <tt>id</tt> of this <tt>Volume</tt>.  The <tt>id</tt> is
     * a 64-bit long that uniquely identifies this volume.
     * @return  The id value
     */
    public long getId()
    {
        return _id;
    }
    
    /**
     * Returns the path name by which this volume was opened.
     * @return  The path name
     */
    public String getPathName()
    {
        return _pathName;
    }
    
    /**
     * Returns the "friendly" name specified for the Volume, or
     * <tt>null</tt> if there is none.  The alias is specified
     * by the optional <tt>alias</tt> attribute of the volume 
     * specification in the Persistit configuration properties.
     * 
     * @return  The alias, or <tt>null</tt> if there is none.
     */
    public String getAlias()
    {
        return _alias;
    }
    
    /**
     * Returns the optional drive name supplied in the configuration for this
     * volume. If no drive name was specified, this method returns "unknown"
     *  
     * @return  Name of the drive on which this volume is located.
     */
    public String getDrive()
    {
        return _drive;
    }
    
    /**
     * Returns the buffer size for this volume, one of 1024, 2048, 4096, 8192
     * or 16384.
     * @return  The buffer size.
     */
    public int getBufferSize()
    {
        return _bufferSize;
    }
    
    /**
     * Returns the page address of the garbage tree.  This method is useful
     * to diagnostic utility programs.
     * @return  The page address
     */
    public long getGarbageRoot()
    {
        synchronized(_lock)
        {
            return _garbageRoot;
        }
    }
    
    private void setGarbageRoot(long garbagePage)
    throws RetryException, InUseException, ReadOnlyVolumeException
    {
        synchronized(_lock)
        {
            _garbageRoot = garbagePage;
            checkpointMetaData();
        }
    }
    
    /**
     * Returns the directory <tt>Tree</tt> for this volume
     * @return  The directory <tt>Tree</tt>
     */
    public Tree getDirectoryTree()
    {
        synchronized(_lock)
        {
            return _directoryTree;
        }
    }
    
    /**
     * Sets the directory <tt>Tree</tt> for this <tt>Volume</tt>.
     * @param tree      The <tt>Tree</tt>
     * @throws RetryException
     * @throws InUseException
     */
    void setDirectoryTree(Tree tree)
    throws RetryException, InUseException, ReadOnlyVolumeException
    {
        claimHeadBuffer(true);
        try
        {
            _pwj.reserve(_headBuffer, null);
            _directoryTree = tree;
            _directoryRootPage = tree.getRootPageAddr();
            checkpointMetaData();
            _headBuffer.dirty();
        }
        finally
        {
            releaseHeadBuffer();
        }
    }
    
    /**
     * Returns the count of physical disk read requests performed on this
     * <tt>Volume</tt>.
     * @return  The count
     */
    public long getReadCounter()
    {
        synchronized(_lock)
        {
            return _readCounter;
        }
    }
    
    /**
     * Returns the count of physical disk write requests performed on this
     * <tt>Volume</tt>.
     * @return  The count
     */
    public long getWriteCounter()
    {
        synchronized(_lock)
        {
            return _writeCounter;
        }
    }

    /**
     * Returns the count of logical buffer fetches performed against this
     * <tt>Volume</tt>.  The ratio of get to read operations indicates how
     * effectively the buffer pool is reducing disk I/O.
     * @return  The count
     */
    public long getGetCounter()
    {
        synchronized(_lock)
        {
            return _getCounter;
        }
    }
    
    /**
     * Returns the count of {@link Exchange#fetch} operations.  These include
     * {@link Exchange#traverse}, {@link Exchange#fetchAndStore} and
     * {@link Exchange#fetchAndRemove} operations. This count is maintained
     * with the stored Volume and is not reset when Persistit closes.
     * It is provided to assist application performance tuning.
     * 
     * @return  The count of records fetched from this Volume.
     */
    public long getFetchCounter()
    {
        synchronized(_lock)
        {
            return _fetchCounter;
        }
    }
    
    /**
     * Returns the count of {@link Exchange#traverse} operations.  These include
     * {@link Exchange#next} and {@link Exchange#previous} operations.
     * This count is maintained  with the stored Volume and is not reset 
     * when Persistit closes. It is provided to assist application 
     * performance tuning.
     * 
     * @return  The count of key traversal operations performed on this
     *          in this Volume.
     */
    public long getTraverseCounter()
    {
        synchronized(_lock)
        {
            return _traverseCounter;
        }
    }
    
    /**
     * Returns the count of {@link Exchange#store} operations, including
     * {@link Exchange#fetchAndStore} and {@link Exchange#incrementValue}
     * operations. This count is maintained with the 
     * stored Volume and is not reset when Persistit closes.
     * It is provided to assist application performance tuning.
     * 
     * @return  The count of records fetched from this Volume.
     */
    public long getStoreCounter()
    {
        synchronized(_lock)
        {
            return _storeCounter;
        }
    }
    
    /**
     * Returns the count of {@link Exchange#remove} operations, including
     * {@link Exchange#fetchAndRemove} operations. This count is maintained
     * with the stored Volume and is not reset when Persistit closes.
     * It is provided to assist application performance tuning.
     * 
     * @return  The count of records fetched from this Volume.
     */
    public long getRemoveCounter()
    {
        synchronized(_lock)
        {
            return _removeCounter;
        }
    }
    
    /**
     * Returns the time at which this <tt>Volume</tt> was created.
     * @return  The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getCreateTime()
    {
        synchronized(_lock)
        {
            return _createTime;
        }
    }
    
    /**
     * Returns the time at which this <tt>Volume</tt> was last opened.
     * @return  The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getOpenTime()
    {
        synchronized(_lock)
        {
            return _openTime;
        }
    }
    
    /**
     * Returns the time at which the last physical read operation was
     * performed on <tt>Volume</tt>.
     * @return  The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getLastReadTime()
    {
        synchronized(_lock)
        {
            return _lastReadTime;
        }
    }
    
    /**
     * Returns the time at which the last physical write operation was
     * performed on <tt>Volume</tt>.
     * @return  The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getLastWriteTime()
    {
        synchronized(_lock)
        {
            return _lastWriteTime;
        }
    }
    
    /**
     * Returns the time at which this <tt>Volume</tt> was last extended
     * (increased in physical size).
     * @return  The time, in milliseconds since January 1, 1970, 00:00:00 GMT.
     */
    public long getLastExtensionTime()
    {
        synchronized(_lock)
        {
            return _lastExtensionTime;
        }
    }
    
    /**
     * Returns the last <tt>IOException</tt> that was encountered while reading,
     * writing, extending or closing the underlying volume file.  Returns
     * <tt>null</tt> if there have been no <tt>IOException</tt>s since
     * the volume was opened.  If <tt>reset</tt> is <tt>true</tt>, the
     * lastException field is cleared so that a subsequent call to this method
     * will return <tt>null</tt> unless another <tt>IOException</tt> has
     * occurred.
     * 
     * @param reset     If <tt>true</tt> then this method clears the
     *                  last exception field
     *                  
     * @return          The most recently encountered <tt>IOException</tt>, or
     *                  <tt>null</tt> if there has been none.
     */
    public IOException lastException(boolean reset)
    {
        IOException ioe = _lastIOException;
        if (reset) _lastIOException = null;
        return ioe;
    }
    
    long getPageCount()
    {
        return _pageCount;
    }
    
    long getMaximumPageInUse()
    {
        return _highestPageUsed;
    }
    
    long getInitialpages()
    {
        return _initialPages;
    }
    
    long getMaximumPages()
    {
        return _maximumPages;
    }
    
    long getExtensionPages()
    {
        return _extensionPages;
    }
    
    void bumpGetCounter()
    {
        synchronized(_lock)
        {
            _getCounter++;
        }
    }
    
    void bumpFetchCounter()
    {
        synchronized(_lock)
        {
            _fetchCounter++;
        }
    }
    
    void bumpTraverseCounter()
    {
        synchronized(_lock)
        {
            _fetchCounter++;
        }
    }
    
    void bumpStoreCounter()
    {
        synchronized(_lock)
        {
            _storeCounter++;
        }
    }
    
    void bumpRemoveCounter()
    {
        synchronized(_lock)
        {
            _removeCounter++;
        }
    }
    
    private String garbageBufferInfo(Buffer buffer)
    {
        if (buffer.getPageType() != Buffer.PAGE_TYPE_GARBAGE)
        {
            return "!!!" + buffer.getPageAddress() + " is not a garbage page!!!";
        }
        return "@<" + buffer.getPageAddress() + ":" + buffer.getAlloc() + ">";
    }
    
    void deallocateGarbageChain(int treeIndex, long left, long right)
    throws PersistitException
    {
        if (Debug.ENABLED) Debug.$assert(left > 0);
        
        claimHeadBuffer(true); 
        
        Buffer garbageBuffer = null;
        boolean garbageBufferReserved = false;
        boolean headBufferReserved = false;
        
        try
        {
            long garbagePage = getGarbageRoot();
            if (garbagePage != 0)
            {
                if (Debug.ENABLED)
                {
                    Debug.$assert(left != garbagePage &&
                                  right != garbagePage);
                }
                
                garbageBuffer = _pool.get(this, garbagePage, true, true);
                try
                {
                    _pwj.reserve(garbageBuffer, null);
                    garbageBufferReserved = true;
                }
                catch (RetryException re)
                {
                    if (_persistit.getLogBase().isLoggable(LogBase.LOG_DEALLOCGC1))
                    {
                        _persistit.getLogBase().log(LogBase.LOG_DEALLOCGC1, garbageBuffer);
                    }
                    throw re;
                } 
                boolean fits =
                    garbageBuffer.addGarbageChain(treeIndex, left, right, -1);
                    
                if (fits)
                {
                    if (_persistit.getLogBase().isLoggable(LogBase.LOG_DEALLOCGC2))
                    {
                        _persistit.getLogBase().log(LogBase.LOG_DEALLOCGC2,
                            left, right, 0, 0, 0,
                            garbageBufferInfo(garbageBuffer),  null,  null,  null, null);
                    }
                    garbageBuffer.dirty();
                    return;
                }
                else
                {
                    if (_persistit.getLogBase().isLoggable(LogBase.LOG_DEALLOCGC3))
                    {
                        _persistit.getLogBase().log(LogBase.LOG_DEALLOCGC3,
                            left, right, 0, 0, 0,
                            garbageBufferInfo(garbageBuffer),  null,  null,  null, null);
                    }
                    if (garbageBufferReserved && garbageBuffer.isClean())
                    {
                        _pwj.unreserve(garbageBuffer);
                        garbageBufferReserved = false;
                    }
                    _pool.release(garbageBuffer);
                    garbageBuffer = null;
                }
            }
            try
            {
                _pwj.reserve(_headBuffer, null);
                headBufferReserved = true;
            }
            catch (RetryException re)
            {
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_DEALLOCGC4))
                {
                    _persistit.getLogBase().log(LogBase.LOG_DEALLOCGC4, _headBuffer);
                }
                throw re;
            }
            
            boolean solitaire = (right == -1);
            garbageBuffer = _pool.get(this, left, true, !solitaire);
            
            if (Debug.ENABLED) Debug.$assert (
                (garbageBuffer.isDataPage() ||
                garbageBuffer.isIndexPage()) ||
                garbageBuffer.isLongRecordPage() ||
                (solitaire && garbageBuffer.isUnallocatedPage()));
                
            try
            {
                _pwj.reserve(garbageBuffer, _headBuffer);
                garbageBufferReserved = true;
            }
            catch (RetryException re)
            {
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_DEALLOCGC5))
                {
                    _persistit.getLogBase().log(LogBase.LOG_DEALLOCGC5, garbageBuffer);
                }
                throw re;
            } 
            long nextGarbagePage =
                solitaire ? 0 : garbageBuffer.getRightSibling();
                
            if (Debug.ENABLED) Debug.$assert(nextGarbagePage > 0 || right == 0 || solitaire);
            
            harvestLongRecords(
                treeIndex,
                garbageBuffer,
                0,
                Integer.MAX_VALUE);
                
            garbageBuffer.init(Buffer.PAGE_TYPE_GARBAGE, "initGarbage");
            
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_DEALLOCGC6))
            {
                _persistit.getLogBase().log(LogBase.LOG_DEALLOCGC6, garbageBufferInfo(garbageBuffer));
            }
            
            if (!solitaire && nextGarbagePage != right)
            {
                // Will always fit because this is a freshly initialized page
                garbageBuffer.addGarbageChain(treeIndex, nextGarbagePage, right, -1);
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_DEALLOCGC7))
                {
                    _persistit.getLogBase().log(LogBase.LOG_DEALLOCGC7,
                        nextGarbagePage, right, 0, 0, 0,
                        garbageBufferInfo(garbageBuffer), null, null, null, null);
                }
            }
            garbageBuffer.setRightSibling(garbagePage);
            garbageBuffer.dirty();
            setGarbageRoot(garbageBuffer.getPageAddress());
        }
        finally
        {
            if (garbageBuffer != null)
            {
                if (garbageBufferReserved && garbageBuffer.isClean())
                {
                    _pwj.unreserve(garbageBuffer);
                    garbageBufferReserved = false;
                }
                _pool.release(garbageBuffer); 
            }
            if (headBufferReserved && _headBuffer.isClean())
            {
                _pwj.unreserve(_headBuffer);
                headBufferReserved = false;
            }
            releaseHeadBuffer();
        }
    }

    void deallocateGarbageChainDeferred(int treeIndex, long left, long right)
    {
        if (Debug.ENABLED && right != -1)
        {
            Buffer garbageBuffer = null;
            try
            {
                garbageBuffer = _pool.get(this, left, false, true);
                Debug.stateChanged(garbageBuffer, "deallocDeferred", -1);
                Debug.$assert(
                    garbageBuffer != null &&
                    (garbageBuffer.isDataPage() ||
                    garbageBuffer.isIndexPage()) ||
                    garbageBuffer.isLongRecordPage() ||
                    (right == -1 && garbageBuffer.isUnallocatedPage()));
            }
            catch (PersistitException pe)
            {
                // ok if this fails.
            }
            finally
            {
                if (garbageBuffer != null)
                {
                    _pool.release(garbageBuffer);
                    garbageBuffer = null;
                }
            }
        }
        synchronized(_lock)
        {
            if (Debug.ENABLED)
            {
                for (int i = 0; i < _deallocationList.size(); i++)
                {
                    DeallocationChain chain = 
                        (DeallocationChain)_deallocationList.get(i);
                    Debug.$assert(chain._leftPage != left);
                }
            }
            _deallocationList.add(
                new DeallocationChain(
                    treeIndex, left, right));
        }
    }
    

    void harvestLongRecords(int treeIndex, Buffer buffer, int start, int end)
    {
        if (buffer.isDataPage())
        {
            int p1 = buffer.toKeyBlock(start);
            int p2 = buffer.toKeyBlock(end);
            for (int p = p1; p < p2 && p != -1; p = buffer.nextKeyBlock(p))
            {
                long pointer = buffer.fetchLongRecordPointer(p);
                if (pointer != 0)
                {
                    deallocateGarbageChainDeferred(treeIndex, pointer, 0);
                }
            }
        }
    }

    /**
     * Commit all dirty Trees.
     * @throws RetryException
     * @throws PersistitException
     */
    void commitAllTreeUpdates()
    throws RetryException, PersistitException
    {
        for (final Tree tree : _treeNameHashMap.values())
        {
            tree.commit();
        }
    }
    
    /**
     * Commits unwritten page deallocation records to the volume.
     */
    void commitAllDeferredDeallocations()
    throws RetryException, PersistitException
    {
        final ArrayList<DeallocationChain> list;
        synchronized(_lock)
        {
            if (_deallocationList.size() == 0) return;
            list = _deallocationList;
            _deallocationList = new ArrayList<DeallocationChain>();
        }
        
        if (Debug.ENABLED && list.size() > 1)
        {
            int size = list.size();
            for (int i = 0; i < size; i++)
            {
                DeallocationChain chain1 = (DeallocationChain)list.get(i);
                for (int j = i + 1; j < size; j++)
                {
                    DeallocationChain chain2 = (DeallocationChain)list.get(j);
                    Debug.$assert(chain1._leftPage != chain2._leftPage);
                }
            }
        }
        //
        // Now we can work on the deallocations at our leisure.
        //
        try
        {
            while(list.size() > 0)
            {
                DeallocationChain chain =
                    (DeallocationChain)list.get(list.size() - 1);
                if (Debug.ENABLED)
                {
//                    try
//                    {
//                        Buffer garbageBuffer = _pool.get(this, chain._leftPage, false, true);
//                        Debug.stateChanged(garbageBuffer, "commitDeallocDeferred", -1);
//                        Debug.$assert(
//                            garbageBuffer != null &&
//                            (garbageBuffer.isDataPage() ||
//                            garbageBuffer.isIndexPage()) ||
//                            garbageBuffer.isLongRecordPage() ||
//                            (chain._rightPage == -1 && garbageBuffer.isUnallocatedPage()));
//                        garbageBuffer.release();
//                    }
//                    catch (PersistitException pe)
//                    {
//                    }
//                    Debug.stateChanged(chain._leftPage, 29, "initCommitDeallocDeferred", -1, -1);
                }
                deallocateGarbageChain(
                    chain._treeIndex,
                    chain._leftPage,
                    chain._rightPage);
                list.remove(list.size() - 1);
            }
        }
        finally
        {
            if (list.size() > 0)
            {
                // Reinsert DeallocationChain records that didn't
                // get processed.  We'll deallocate them on the next
                // invocation.
                //
                synchronized(_lock)
                {
                    if (_deallocationList.size() == 0)
                    {
                        _deallocationList = list;
                    }
                    else
                    {
                        _deallocationList.addAll(list);
                    }
                }
            }
        }
    }
    
    Exchange directoryExchange()
    {
        Exchange ex = new Exchange(_persistit, _directoryTree);
        ex.ignoreTransactions();
        return ex;
    }
    
    /**
     * Looks up by name and returns a <tt>Tree</tt> within this <tt>Volume</tt>.
     * If no such tree exists, this method either creates a new tree or 
     * returns null depending on whether the <tt>createIfNecessary</tt>
     * parameter is <tt>true</tt>.
     * 
     * @param name      The tree name
     * 
     * @param createIfNecessary
     *                  Determines whether this method will create a
     *                  new tree if there is no tree having the specified name.
     *  
     * @return          The <tt>Tree</tt>, or <tt>null</tt> if there is no
     *                  such tree in this <tt>Volume</tt>.
     * 
     * @throws          PersistitException
     */
    public Tree getTree(String name, boolean createIfNecessary)
    throws PersistitException
    {
        Tree tree = null;
        boolean virgin = false;
        boolean claimed = false;
        long expirationTime = 0;
        
        if (name.equals(DIRECTORY_TREE_NAME))
        {
            throw new IllegalArgumentException(
                "Reserved tree name: " + name);
        }
        for (;;)
        {
            try
            {
                synchronized(_lock)
                {
                    tree = (Tree)_treeNameHashMap.get(name);
                    if (tree == null)
                    {
                        tree = new Tree(_persistit, this);
                        claimed = tree.claim(true);   // will always succeed
                        if (Debug.ENABLED) Debug.$assert(claimed);
                        
                        _treeNameHashMap.put(name, tree);
                        virgin = true;
                    }
                }
                
                // At this point, we have a Tree object with a writer
                // claim on it, and it is in the map.
                try
                {
                    if (!virgin)
                    {
                        // We need to get a temporary claim on the Tree so that
                        // we do not read an uncommitted new tree structure's data.
                        //
                        if (tree.claim(true))
                        {
                            claimed = true;
                            if (!tree.isValid()) return null;
                        }
                        else
                        {
                            throw new InUseException(
                                "Thread " + Thread.currentThread().getName() + 
                                " could not get reader claim on " + tree);
                        } 
                    }
                    
                    else
                    {
                        Exchange ex = directoryExchange();
                        ex.clear().append(DIRECTORY_TREE_NAME).append(BY_NAME).append(name);
                        Value value = ex.fetch().getValue();
                        
                        if (value.isDefined()) 
                        {
                        	tree.load(value);
                        }
                        else if (createIfNecessary) 
                        {
                        	createTree(name, tree);
                        }
                        else
                        {
                        	return null;
                        }
                        tree.setValid(true);
                        virgin = false;
                    }
                    return tree;
                }
                finally
                {
                    if (virgin)
                    {
                        synchronized(_lock)
                        {
                            if (claimed) tree.release();
                            _treeNameHashMap.remove(name);
                            tree = null;
                        }
                    }
                    else
                    {
//                      PDB 11/22/04 - removed this because it forces us to
//                      commit every time we do a getTree after any store().
//                      Need to confirm that this isn't needed.  Note that if
//                      We called createTree() above, the tree was committed 
//                      then.
//                        if (tree != null && 
//                            tree.isDirty() && 
//                            createIfNecessary)
//                        {
//                            tree.commit();
//                        }
                        if (claimed) tree.release();
                        claimed = false;
                    }
                }
            }
            catch(RetryException re)
            {
                if (expirationTime == 0)
                {
                    expirationTime =
                        System.currentTimeMillis() +
                        Persistit.DEFAULT_TIMEOUT_VALUE; 
                }                        
                else if (System.currentTimeMillis() < expirationTime)
                {
                    _persistit.getPrewriteJournal().waitForReservation(re);
                }
                else throw new TimeoutException("Failed to create new Tree");
            }
        }
    }
    
    
    void updateTree(Tree tree)
    throws PersistitException
    {
        if (tree == _directoryTree)
        {
            claimHeadBuffer(true);
            try
            {
                _pwj.reserve(_headBuffer, null);
                _directoryRootPage = tree.getRootPageAddr();
                checkpointMetaData();
                _headBuffer.dirty();
            }
            finally
            {
                releaseHeadBuffer();
            }
        }
        else
        {
            Exchange ex = directoryExchange();
            tree.store(ex.getValue());
            ex.clear()
                .append(DIRECTORY_TREE_NAME)
                .append(BY_NAME)
                .append(tree.getName())
                .store();
            ex.getValue().put(null);
            ex.clear()
                .append(DIRECTORY_TREE_NAME)
                .append(BY_INDEX)
                .append(tree.getTreeIndex())
                .append(tree.getName())
                .store();
        }
    }
    
    /**
     * Removes a <tt>Tree</tt> and makes all the index and data pages
     * formerly associated with that <tt>Tree</tt> available for reuse.
     * @param treeName  The name of the <tt>Tree</tt> to remove.
     * @return  <tt>true</tt> if a there was a <tt>Tree</tt> of the specified   
     *          name and it was removed, otherwise <tt>false</tt>.
     * @throws PersistitException
     */
    boolean removeTree(String treeName)
    throws PersistitException
    {
        Tree tree = null;
        synchronized(_lock)
        {
            tree = (Tree)_treeNameHashMap.get(treeName);
        }
        if (tree != null) return removeTree(tree);
        else return false;
    }
    
    boolean removeTree(Tree tree)
    throws PersistitException
    {
        if (tree == _directoryTree)
        {
            throw new IllegalArgumentException("Can't delete the Directory tree");
        }
        _persistit.suspend();
        
        int treeIndex = -1;
        int depth = -1;
        long page = -1;
        
        tree.claim(true);
        try
        {
            if (!tree.isInitialized()) return false;

            long journalId = 
                _persistit.getJournal().beginRemoveTree(tree);
            
            long rootPage = tree.getRootPageAddr();
            page = rootPage;
            treeIndex = tree.getTreeIndex();
            depth = tree.getDepth();
            
            Exchange ex = directoryExchange();
            
            ex.clear()
                .append(DIRECTORY_TREE_NAME)
                .append(BY_NAME)
                .append(tree.getName())
                .remove();
            
            ex.clear()
                .append(DIRECTORY_TREE_NAME)
                .append(BY_INDEX)
                .append(tree.getTreeIndex())
                .append(tree.getName())
                .remove();

            synchronized(_lock)
            {
                _treeNameHashMap.remove(tree.getName());
                _treeIndexHashMap.remove(new Integer(treeIndex));
                tree.invalidate();
            }
            
            _persistit.getJournal().completed(journalId);
        }
        finally
        {
            tree.release();
        }
        // The Tree is now gone.  The following deallocates the
        // pages formerly associated with it. If this fails we'll be
        // left with allocated pages that are not available on the garbage
        // chain for reuse.
        
        long expirationTime = 0;
        for (;;)
        {
            try
            {
                while (page != -1)
                {
                    Buffer buffer = null;
                    try
                    {
                        buffer = _pool.get(this, page, false, true);
                        if (buffer.getPageType() != depth)
                        {
                            throw new CorruptVolumeException(
                                "Page " + buffer + 
                                " type code=" + buffer.getPageType() +
                                " is not equal to expected value " + depth);
                        }
                        deallocateGarbageChainDeferred(treeIndex, page, 0);
                        if (buffer.isIndexPage())
                        {
                            int p = buffer.toKeyBlock(0);
                            if (p > 0) page = buffer.getPointer(p);
                            else page = -1;
                            depth--;
                        }
                        else if (buffer.isDataPage())
                        {
                            page = -1;
                            break;
                        }
                    }
                    finally
                    {
                        if (buffer != null) _pool.release(buffer);
                        buffer = null;
                    }
                }
            
                commitAllDeferredDeallocations();
                return true;
            }
            catch (RetryException re)
            {
                if (expirationTime == 0)
                {
                    expirationTime =
                        System.currentTimeMillis() +
                        Persistit.DEFAULT_TIMEOUT_VALUE; 
                }                        
                else if (System.currentTimeMillis() < expirationTime)
                {
                    _persistit.getPrewriteJournal().waitForReservation(re);
                }
                else throw new TimeoutException("Failed to create new Tree");
            }
        }
    }
    
    /**
     * Returns an array of all currently defined <tt>Tree</tt> names.
     * 
     * @return          The array
     * 
     * @throws          PersistitException
     */
    public String[] getTreeNames()
    throws PersistitException
    {
        Vector vector = new Vector();
        Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(BY_NAME).append("");
        while (ex.next())
        {
            String treeName = ex.getKey().indexTo(-1).decodeString();
            vector.add(treeName);
        }
        String[] names = new String[vector.size()];
        for (int index = 0; index < names.length; index++)
        {
            names[index] = (String)vector.elementAt(index);
        }
        return names;
    }
    
    /**
     * Returns the next tree name in alphabetical order within this volume.
     * 
     * @param treeName  The starting tree name
     * 
     * @return          The name of the first tree in alphabetical order
     *                  that is larger than <tt>treeName</tt>.
     *                  
     * @throws PersistitException
     */
    String nextTreeName(String treeName)
    throws PersistitException
    {
        Exchange ex = directoryExchange();
        ex.clear().append(DIRECTORY_TREE_NAME).append(BY_NAME).append(treeName);
        if (ex.next())
        {
            return ex.getKey().indexTo(-1).decodeString();
        }
        return null;
    }
    /**
     * Returns an array of all Trees currently open within this Volume.
     * @return  The array.
     */
    Tree[] getTrees()
    {
        int size = _treeNameHashMap.values().size();
        Tree[] trees = new Tree[size];
        int index = 0;
        for (final Tree tree : _treeNameHashMap.values()) {
            trees[index++] = tree;
        }
        return trees;
    }
    
    /**
     * Return a TreeInfo structure for a tree by the specified name.
     * If there is no such tree, then return <i>null</i>. 
     * @param   tree name
     * @return  an information structure for the Management interface.
     */
    Management.TreeInfo getTreeInfo(String name)
    {
        Tree tree;
        boolean virgin = false;
        try
        {
            synchronized(_lock)
            {
                tree = (Tree)_treeNameHashMap.get(name);
                if (tree == null)
                {
                    tree = new Tree(_persistit, this);
                    virgin = true;
                }
            }
            
            if (virgin)
            {
                Exchange ex = directoryExchange();
                ex.clear().append(DIRECTORY_TREE_NAME).append(BY_NAME).append(name);
                Value value = ex.fetch().getValue();
                if (value.isDefined()) tree.load(value);
                tree.setValid(true);
                virgin = false;
            }
            return new Management.TreeInfo(tree);
        }
        catch (PersistitException pe)
        {
            return null;
        }
    }
    
    /**
     * Indicates whether this <tt>Volume</tt> has been closed.
     * @return  <i>true</i> if this Volume is closed.
     */
    public boolean isClosed()
    {
        return _closed;
    }
    
    /**
     * Indicates whether this <tt>Volume</tt> prohibits updates.
     * @return  <i>true</i> if this Volume prohibits updates.
     */
    public boolean isReadOnly()
    {
        return _readOnly;
    }
    
    /**
     * Indicates whether this is a temporary <tt>Volume</tt>.  If so, its
     * state will be reinitialized to empty each time the Volume is opened.
     * In addition, Persistit may use a faster I/O path to write dirty data
     * to disk since preservation of integrity after a dirty shutdown does
     * not matter.
     * @return  <t>true</tt> if this is a temporary <tt>Volume</tt>
     */
    public boolean isTemporary()
    {
        return _temporary;
    }
    /**
     * Create a new tree in this volume.  A tree is represented by an
     * index root page and all the index and data pages pointed to by that
     * root page.
     * @return newly create Tree object
     * @throws PersistitException
     */
    private Tree createTree(String treeName, Tree tree)
    throws PersistitException
    {
        _persistit.suspend();
        Buffer rootPageBuffer = null;

        int treeIndex = 0;

        if (!treeName.equals(DIRECTORY_TREE_NAME))
        {
            synchronized(_lock)
            {
                treeIndex = ++_maxTreeIndex;
            }
        }
        
        long journalId =
            _persistit.getJournal()
                .beginCreateTree(this, treeName, treeIndex);

        rootPageBuffer = allocPage(treeIndex);
        long rootPage = rootPageBuffer.getPageAddress();
        
        try
        {
            _pwj.reserve(rootPageBuffer, null);
            rootPageBuffer.init(Buffer.PAGE_TYPE_DATA, "initRootPage");
            rootPageBuffer.putValue(Key.LEFT_GUARD_KEY, Value.EMPTY_VALUE);
            rootPageBuffer.putValue(Key.RIGHT_GUARD_KEY, Value.EMPTY_VALUE);
            rootPageBuffer.dirty();
        }
        catch (RetryException re)
        {
            synchronized(_lock)
            {
                if (treeIndex == _maxTreeIndex) _maxTreeIndex--;
            }
            deallocateGarbageChainDeferred(0, rootPageBuffer.getPageAddress(), -1);
            
            throw re;
        }
        finally
        {
            _pool.release(rootPageBuffer);
        }
        
        tree.initialize(treeName, treeIndex);
        _treeIndexHashMap.put(new Integer(treeIndex), tree);
        tree.claim(true);
        tree.changeRootPageAddr(rootPage, 1);
        tree.release();
        tree.setValid(true);
        tree.commit();
        
        if (journalId != -1)
        {
            _persistit.getJournal().completed(journalId);
        }
        
        return tree;
    }

    /**
     * Get a RandomAccessFile object with which to perform reads on this Volume.
     * Note that there may be multiple Threads performing reads concurrently
     * on separate RandomAccessFile objects referring to the same file.
     * <p>
     * This method should always be followed by releaseRaf() to allow reuse of
     * the RandomAccessFile object that was obtained.
     * 
     * @return a RandomAccessFile to perform reads on this Volume.
     * @throws IOException
     * @throws VolumeClosedException
     */
    private RandomAccessFile getRaf()
    throws VolumeClosedException, PersistitIOException
    {
        synchronized(_rafStack)
        {
            if (_closed)
            {
                throw new VolumeClosedException(this.getPathName());
            } 
            if (!_rafStack.isEmpty())
            {
                return (RandomAccessFile)_rafStack.pop();
            }
        }
        try
        {
            return new RandomAccessFile(_pathName, "r");
        }
        catch (IOException ioe)
        {
            throw new PersistitIOException(ioe);                    
        }
    }
    
    /**
     * Release a RandomAccessFile obtained from getRaf().
     * @param raf   The RandomAccessFile obtained previously from getRaf().
     */
    private void releaseRaf(RandomAccessFile raf)
    {
        synchronized(_rafStack)
        {
            _rafStack.push(raf);
        }
    }
    
    /**
     * Get a RandomAccessFile object with which to perform I/O on this Volume.
     * Note that there may be multiple Threads performing I/O concurrently
     * on separate RandomAccessFile objects referring to the same file.
     * Apparently this is okay, at least on Windows.
     * <p>
     * This method should always be followed by releaseRaf() to allow reuse of
     * the RandomAccessFile object that was obtained.
     * 
     * @return a RandomAccessFile to perform I/O on this Volume.
     * @throws IOException
     * @throws VolumeClosedException
     */
    private RandomAccessFile getRafRW()
    throws VolumeClosedException, ReadOnlyVolumeException, IOException
    {
        if (_readOnly)
        {
            throw new ReadOnlyVolumeException();
        }
        synchronized(_rafStackRW)
        {
            if (_closed)
            {
                throw new VolumeClosedException(this.getPathName());
            } 
            if (!_rafStackRW.isEmpty())
            {
                return (RandomAccessFile)_rafStackRW.pop();
            }
        }
        return new RandomAccessFile(_pathName, "rw");
    }
    
    /**
     * Release a RandomAccessFile obtained from getRafRW().
     * @param raf   The RandomAccessFile obtained previously from getRafRW().
     */
    private void releaseRafRW(RandomAccessFile raf)
    {
        synchronized(_rafStackRW)
        {
            _rafStackRW.push(raf);
        }
    }
    
    /**
     * Returns the <tt>BufferPool</tt> in which this volume's pages are
     * cached.
     * @return  This volume's </tt>BufferPool</tt>
     */
    BufferPool getPool()
    {
        return _pool;
    }
    
    private void claimHeadBuffer(boolean writer)
    throws InUseException
    {
        if (!_headBuffer.claim(true))
        {
            throw new InUseException(
                this + " head buffer " + _headBuffer + " is unavailable");
        }
    }
    
    private void releaseHeadBuffer()
    {
        _pool.release(_headBuffer);
    }

    
    void readPage(Buffer buffer, long page)
    throws PersistitIOException, InvalidPageAddressException,
           VolumeClosedException, RetryException
    {
        if (page < 0 || page >= _pageCount)
        {
            throw new InvalidPageAddressException("Page " + page);
        }
        Debug.IOLogEvent iev = null;
        if (Debug.IOLOG_ENABLED)
        {
            iev = Debug.startIOEvent("readPage", page);
        }
        RandomAccessFile raf = null;
        Exception exception = null;
        try
        {
            raf = getRaf();
            raf.seek(page * _bufferSize);
            raf.read(buffer.getBytes());
            synchronized(_lock)
            {
                _lastReadTime = System.currentTimeMillis();
                _readCounter++;
            }
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_READ_OK))
            {
                _persistit.getLogBase().log(LogBase.LOG_READ_OK, page, buffer.getIndex());
            }
        }
        catch (IOException ioe)
        {
            
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_READ_IOE))
            {
                _persistit.getLogBase().log(LogBase.LOG_READ_IOE,
                    page, buffer.getIndex(), 0, 0, 0,
                    ioe, null, null, null, null);
            }
            _lastIOException = ioe;
            exception = ioe;
            
            if (_persistit.isReadRetryEnabled())
            {
                throw _pwj.getRetryException();
            }
            else
            {
                throw new PersistitIOException(ioe);
            }
        }
        finally
        {
            if (raf != null) releaseRaf(raf);
            if (Debug.IOLOG_ENABLED)
            {
                Debug.endIOEvent(iev, exception);
            }
        }
    }
    
    void writePage(byte[] bytes, int offset, int length, long page)
    throws IOException, InvalidPageAddressException,
           ReadOnlyVolumeException, VolumeClosedException
    {
        long elapsed = System.nanoTime();
        if (_readOnly)
        {
            throw new ReadOnlyVolumeException(this.getPathName());
        }
        
        Debug.IOLogEvent iev = null;
        if (Debug.IOLOG_ENABLED)
        {
            iev = Debug.startIOEvent("writePage", page);
        }
        
        RandomAccessFile raf = null;
        Exception exception = null;
        try
        {
            raf = getRafRW();
            raf.seek(page * _bufferSize);
            raf.write(bytes, offset, length);
            synchronized(_lock)
            {
                _lastWriteTime = System.currentTimeMillis();
                _writeCounter++;
            }
        }
        catch (IOException ioe)
        {
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_WRITE_IOE))
            {
                _persistit.getLogBase().log(LogBase.LOG_WRITE_IOE,
                    page, 0, 0, 0, 0,
                    ioe, null, null, null, null);
            }
            _lastIOException = ioe;
            exception = ioe;
            throw ioe;
        }
        finally
        {
            if (raf != null) releaseRafRW(raf);
            if (Debug.IOLOG_ENABLED)
            {
                Debug.endIOEvent(iev, exception);
            }
        }
        
        if (DEBUG_WRITE_LOG) {
	        synchronized(_lock) {
	        	if (printWriter == null) {
	        		final File file = new File(this._pathName + "_writeLog");
	        		printWriter = new PrintWriter(new BufferedWriter(new FileWriter(file)));
	        	}
	        	elapsed = System.nanoTime() - elapsed;
	        	printWriter.printf("Write page=%8d, delta=%8d usec=%8d\n", page, page - lastPageWritten, elapsed / 1000);
	        	lastPageWritten = page;
	        	if (_lastWriteTime - lastFlushTime > 5000) {
	        		printWriter.flush();
	        		lastFlushTime = _lastWriteTime;
	        	}
	        }
        }
    }
    
    /**
     * Allocates a previously unused page.  Returns a Buffer containing that
     * page.  Empties all previous content of that page and sets its type to
     * UNUSED.
     * @param   treeIndex The tree in which this page will be used
     * @return  a Buffer containing the newly allocated page.
     *          The returned buffer has a writer claim on it.
     */
    Buffer allocPage(int treeIndex)
    throws PersistitException, RetryException
    {
        Buffer buffer = null;
        boolean headReserved = false;
        boolean garbageBufferReserved = false;
        
        // First we attempt to allocate from the uncommitted deallocation list
        DeallocationChain dc = null;
        synchronized(_lock)
        {
            final ArrayList<DeallocationChain> list = _deallocationList;
            if (list != null && list.size() > 0)
            {
            	dc = list.remove(list.size() - 1);
            }
        }
        if (dc != null)
        {
            long page = dc._leftPage;
            long rightPage = dc._rightPage;
            boolean solitaire = rightPage == -1;
            if (page != rightPage)
            {
                try
                {
                    buffer = _pool.get(this, page, true, !solitaire);
                    if (!solitaire && buffer.getRightSibling() != rightPage)
                    {
                        dc._leftPage = buffer.getRightSibling();
                    }
                    else dc = null;
                }
//                catch (PersistitException pe)
//                {
//                }
                //
                // PDB 20050726 - Retry Exception should be thrown rather than
                //              get eaten
                //
                finally
                {
                    if (dc != null)
                    {
                        synchronized(_lock)
                        {
                            _deallocationList.add(dc);
                        }
                    }
                }
            }
        }
        
        if (buffer != null)
        {
            harvestLongRecords(
                treeIndex,
                buffer,
                0,
                Integer.MAX_VALUE);
            
            buffer.init(Buffer.PAGE_TYPE_UNALLOCATED, "initFromGarbage");   //DEBUG - debug
            //
            // PDB 20050726 - Possible source of wrong page type on deferred dealloc
            // PDB 20050727 - can't reserve here; caller must do it!
            //_pwj.reserve(buffer, null);
            buffer.clear();
            //buffer.dirty();
            return buffer;
        }
        
        // Okay, next we look at the stored garbage chain for this Volume.
        claimHeadBuffer(true);
        try
        {
            long garbageRoot = getGarbageRoot();
            if (garbageRoot != 0)
            {
                Buffer garbageBuffer = _pool.get(this, garbageRoot, true, true);

                try
                {
                    if (Debug.ENABLED) Debug.$assert(garbageBuffer.isGarbagePage());
                    if (Debug.ENABLED) Debug.$assert(
                        (garbageBuffer.getStatus() & CLAIMED_MASK) == 1);
                    
                    try
                    {
                         _pwj.reserve(garbageBuffer, null);
                        garbageBufferReserved = true;
                    }
                    catch (RetryException re)
                    {
                        if (_persistit.getLogBase().isLoggable(LogBase.LOG_ALLOC_RETRY))
                        {
                            _persistit.getLogBase().log(LogBase.LOG_ALLOC_RETRY, garbageBuffer);
                        }
                        throw re;
                    } 
                    
                    long page = garbageBuffer.getGarbageChainLeftPage();
                    long rightPage = garbageBuffer.getGarbageChainRightPage(); 
                    
                    if (Debug.ENABLED) Debug.$assert(page != 0);
                    
                    if (page == -1)
                    {
                        long newGarbageRoot = garbageBuffer.getRightSibling();
                        if (_persistit.getLogBase().isLoggable(LogBase.LOG_ALLOC_GARROOT))
                        {
                            _persistit.getLogBase().log(LogBase.LOG_ALLOC_GARROOT,
                                garbageRoot, newGarbageRoot, 0, 0, 0,
                                garbageBufferInfo(garbageBuffer), null, null, null, null);
                        }
                        try
                        {
                            _pwj.reserve(_headBuffer, garbageBuffer);
                            headReserved = true;
                        }
                        catch (RetryException re)
                        {
                            if (garbageBuffer.isClean())
                            {
                                _pwj.unreserve(garbageBuffer);
                                garbageBufferReserved = false;
                            }
                            if (_persistit.getLogBase().isLoggable(LogBase.LOG_ALLOC_RETRY))
                            {
                                _persistit.getLogBase().log(LogBase.LOG_ALLOC_RETRY, _headBuffer);
                            }
                            throw re;
                        }
                        setGarbageRoot(newGarbageRoot);
                        buffer = garbageBuffer;
                        garbageBuffer = null;
                        garbageBufferReserved = false;
                    }
                    else
                    {
                        int garbageTreeIndex =
                            garbageBuffer.getGarbageChainTreeIndex();
                            
                        if (_persistit.getLogBase().isLoggable(LogBase.LOG_ALLOC_GAR))
                        {
                            _persistit.getLogBase().log(LogBase.LOG_ALLOC_GAR,
                                page, 0, 0, 0, 0,
                                garbageBufferInfo(garbageBuffer), null, null, null, null);
                        }
                        boolean solitaire = rightPage == -1;
                        buffer = _pool.get(this, page, true, !solitaire);
                        
                        try
                        {
                            _pwj.reserve(buffer, garbageBuffer);
                        }
                        catch (RetryException re)
                        {
                            _pool.release(buffer);
                            buffer = null;
                            throw re;
                        }
                        
                        if (Debug.ENABLED) Debug.$assert(buffer.getPageAddress() > 0);
                        
                        long nextGarbagePage =
                            solitaire ? -1 : buffer.getRightSibling();

                        if (nextGarbagePage == rightPage ||
                            nextGarbagePage == 0)
                        {
                            if (_persistit.getLogBase().isLoggable(LogBase.LOG_ALLOC_GAR_END))
                            {
                                _persistit.getLogBase().log(LogBase.LOG_ALLOC_GAR_END,
                                    rightPage, 0, 0, 0, 0,
                                    garbageBufferInfo(garbageBuffer), null, null, null, null);
                            }
                            garbageBuffer.removeGarbageChain();
                        }
                        else
                        {
                            if (_persistit.getLogBase().isLoggable(LogBase.LOG_ALLOC_GAR_UPDATE))
                            {
                                _persistit.getLogBase().log(LogBase.LOG_ALLOC_GAR_UPDATE,
                                    nextGarbagePage, rightPage, 0, 0, 0,
                                    garbageBufferInfo(garbageBuffer), null, null, null, null);
                            }
                            
                            if (Debug.ENABLED) Debug.$assert(nextGarbagePage > 0);
                            
                            garbageBuffer.setGarbageLeftPage(
                                garbageTreeIndex,
                                nextGarbagePage);
                        }
                    }
                    if (Debug.ENABLED)
                    {
                        Debug.$assert(
                            buffer != null && 
                            buffer.getPageAddress() != 0 &&
                            buffer.getPageAddress() != _garbageRoot &&
                            buffer.getPageAddress() != _directoryRootPage);
                    }
                    
                    harvestLongRecords(
                        treeIndex,
                        buffer,
                        0,
                        Integer.MAX_VALUE);
                    
                    buffer.init(Buffer.PAGE_TYPE_UNALLOCATED, "initFromGarbage");   //DEBUG - debug
                    buffer.clear();
                    //buffer.dirty();
                    return buffer;
                }
                finally
                {
                    if (garbageBufferReserved && garbageBuffer.isClean())
                    {
                        _pwj.unreserve(garbageBuffer);
                    }
                    if (garbageBuffer != null)
                    {
                        if (buffer != null) {
                            _persistit.getLockManager().setOffset(); 
                        }
                        _pool.release(garbageBuffer);
                    }
                }
            }
            else
            {
                if (_firstAvailablePage >= _pageCount)
                {
                    extend();
                }
                
                _pwj.reserve(_headBuffer, null);
                headReserved = true;
                
                long page;
                synchronized(_lock)
                {
                    page = _firstAvailablePage++;
                }
    
                try
                {
                    // No need to read the prior content of the page - we trust
                    // it's never been used before.
                    buffer = _pool.get(this, page, true, false);
                    buffer.init(Buffer.PAGE_TYPE_UNALLOCATED, "initFromExtension"); //DEBUG - debug
                    if (page > _highestPageUsed) _highestPageUsed = page;
                    checkpointMetaData();
                    
                    if (Debug.ENABLED) Debug.$assert(buffer.getPageAddress() != 0);
                    return buffer;
                }
                catch (RetryException rex)
                {
                    synchronized(_lock)
                    {
                        if (page == _firstAvailablePage - 1)
                        {
                            _firstAvailablePage--;
                        }
                    }
                    throw rex;
                }
            }
        }
        finally
        {
            if (headReserved && _headBuffer.isClean())
            {
                _pwj.unreserve(_headBuffer);
            }
            if (buffer != null) {
                _persistit.getLockManager().setOffset();
            }
            releaseHeadBuffer();
        }
    }
    
    void extend()
    throws PersistitException
    {
        extend(_pageCount + _extensionPages);
    }
    
    void extend(long pageCount)
    throws PersistitException
    {
        // No extension required.
        if (pageCount <= _pageCount) return;
        
        // Check for maximum size
        if (_maximumPages <= _pageCount)
        {
            throw new VolumeFullException(this.getPathName());
        }

        // Do no extend past maximum pages
        if (pageCount > _maximumPages)
        {
            pageCount = _maximumPages;
        }
        
        long newLength = pageCount * _bufferSize;
        long currentLength = -1;
        
        claimHeadBuffer(true);
        boolean reserved = false;
        try
        {
            _pwj.reserve(_headBuffer, null);
            reserved = true;
    
            RandomAccessFile raf = getRafRW();
            currentLength = raf.length();
            
            Debug.IOLogEvent iev = null;
            if (Debug.IOLOG_ENABLED)
            {
                iev = Debug.startIOEvent("extend from " + currentLength + " to ", newLength);
            }
            
            if (currentLength > newLength)
            {
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_EXTEND_BADLENGTH))
                {
                    _persistit.getLogBase().log(LogBase.LOG_EXTEND_BADLENGTH, 
                        currentLength, 
                        newLength, 
                        0, 
                        0, 
                        0,
                        this, 
                        null, 
                        null, 
                        null, 
                        null);
                }
            }
            if (currentLength < newLength)
            {
                raf.setLength(newLength);
                if (_persistit.getLogBase().isLoggable(LogBase.LOG_EXTEND_NORMAL))
                {
                    _persistit.getLogBase().log(LogBase.LOG_EXTEND_NORMAL,
                        currentLength,
                        newLength,
                        0,
                        0,
                        0,
                        this,
                        null, 
                        null, 
                        null, 
                        null);
                }
                        
            }
            releaseRafRW(raf);
            
            if (Debug.IOLOG_ENABLED)
            {
                Debug.endIOEvent(iev, null);
            }
            
            synchronized(_lock)
            {
                _pageCount = pageCount;
                _lastExtensionTime = System.currentTimeMillis();
            }
            checkpointMetaData();
        }
        catch (IOException ioe)
        {
            _lastIOException = ioe;
            if (_persistit.getLogBase().isLoggable(LogBase.LOG_EXTEND_IOE))
            {
                _persistit.getLogBase().log(LogBase.LOG_EXTEND_BADLENGTH, 
                    currentLength, 
                    newLength, 
                    0, 
                    0, 
                    0,
                    ioe,
                    this, 
                    null, 
                    null, 
                    null);
            }
            throw new PersistitIOException(ioe);
        }
        finally
        {
            if (reserved && _headBuffer.isClean())
            {
                _pwj.unreserve(_headBuffer);
            }
            releaseHeadBuffer();
        }
    }
    
    void close()
    throws PersistitException
    {
        claimHeadBuffer(true);
        boolean done = _readOnly;
        
        IOException ioe = null;
        
        for (int retry = 0; !done && retry < MAX_CHECKPOINT_RETRIES; retry++)
        {
            try
            {
                commitAllTreeUpdates();
                commitAllDeferredDeallocations();
                _pwj.reserve(_headBuffer, null);
                clean();
                checkpointMetaData();
                done = true;
            }
            catch (RetryException re)
            {
                _persistit.getPrewriteJournal().waitForReservation(re);
            }
        }
        
        if (!done)
        {
            throw new TimeoutException(
                "Could not reserve buffer " + _headBuffer +
                " for " + this);
        }
        
        releaseHeadBuffer();

        _pwj.ensureWritten(this);
        _headBuffer.setPermanent(false);
        _pool.invalidate(this);
        
        synchronized(_lock)
        {
            _closed = true;
            
            while (!_rafStack.isEmpty())
            {
                RandomAccessFile raf =
                    (RandomAccessFile)_rafStack.pop();
                try
                {
                    raf.close();
                }
                catch (IOException e)
                {
                    ioe = e;
                }
            }
            
            while (!_rafStackRW.isEmpty())
            {
                RandomAccessFile raf =
                    (RandomAccessFile)_rafStackRW.pop();
                try
                {
                    raf.getFD().sync();
                    raf.close();
                }
                catch (IOException ioe2)
                {
                    _lastIOException = ioe2;
                    ioe = ioe2;
                }
            }
            _persistit.removeVolume(this, false);
        }
        _pwj = null;
        
        if (ioe != null) throw new PersistitIOException(ioe);
    }
    
    void sync()
    throws PersistitIOException, ReadOnlyVolumeException
    {
        try
        {
            RandomAccessFile raf = getRafRW();
            raf.getFD().sync();
            releaseRafRW(raf);
        }
        catch (VolumeClosedException vce)
        {
            // If the Volume is closed, then we sync'ed it then.
            // Therefore do not throw this Exception, because the caller's
            // intent has been satisfied.
        }
        catch (IOException ioe)
        {
            _lastIOException = ioe;
            throw new PersistitIOException(ioe);
        }
    }
    
    private void putHeaderInfo(byte[] bytes)
    {
        Util.putBytes(bytes,  0, isDirty() ? STATUS_DIRTY : STATUS_CLEAN);
        Util.putBytes(bytes,  8, SIGNATURE);
        Util.putInt(bytes,   16, VERSION);
        Util.putInt(bytes,   20, _bufferSize);
        // 24: long changeCount
        Util.putLong(bytes,  32, _id);
        Util.putLong(bytes,  40, _readCounter);
        Util.putLong(bytes,  48, _writeCounter);
        Util.putLong(bytes,  56, _getCounter);
        Util.putLong(bytes,  64, _openTime);
        Util.putLong(bytes,  72, _createTime);
        Util.putLong(bytes,  80, _lastReadTime);
        Util.putLong(bytes,  88, _lastWriteTime);
        Util.putLong(bytes,  96, _lastExtensionTime);
        Util.putLong(bytes, 104, _highestPageUsed);
        Util.putLong(bytes, 112, _pageCount);
        Util.putLong(bytes, 120, _extensionPages);
        Util.putLong(bytes, 128, _maximumPages);
        Util.putLong(bytes, 136, _firstAvailablePage);
        Util.putLong(bytes, 144, _directoryRootPage);
        Util.putLong(bytes, 152, _garbageRoot);
        Util.putLong(bytes, 160, _fetchCounter);
        Util.putLong(bytes, 168, _traverseCounter);
        Util.putLong(bytes, 176, _storeCounter);
        Util.putLong(bytes, 184, _removeCounter);
        Util.putLong(bytes, 192, _initialPages);
        Util.putByte(bytes, 200, _temporary ? 255 : 0);
    }
    
    private void getHeaderInfo(byte[] bytes)
    {
        _bufferSize         = Util.getInt(bytes,   20);
        // 24: long changeCount
        _id                 = Util.getLong(bytes,  32);
        _readCounter        = Util.getLong(bytes,  40);
        _writeCounter       = Util.getLong(bytes,  48);
        _getCounter         = Util.getLong(bytes,  56);
        _openTime           = Util.getLong(bytes,  64);
        _createTime         = Util.getLong(bytes,  72);
        _lastReadTime       = Util.getLong(bytes,  80);
        _lastWriteTime      = Util.getLong(bytes,  88);
        _lastExtensionTime  = Util.getLong(bytes,  96);
        _highestPageUsed    = Util.getLong(bytes, 104);
        _pageCount          = Util.getLong(bytes, 112);
        _extensionPages     = Util.getLong(bytes, 120);
        _maximumPages       = Util.getLong(bytes, 128);
        _firstAvailablePage = Util.getLong(bytes, 136);
        _directoryRootPage  = Util.getLong(bytes, 144);
        _garbageRoot        = Util.getLong(bytes, 152);
        _fetchCounter       = Util.getLong(bytes, 160);
        _traverseCounter    = Util.getLong(bytes, 168);
        _storeCounter       = Util.getLong(bytes, 176);
        _removeCounter      = Util.getLong(bytes, 184);
        _initialPages       = Util.getLong(bytes, 192);
        _temporary          = Util.getByte(bytes, 200) != 0;
        
        if (bytesEqual(bytes, 0, STATUS_CLEAN))
        {
            clean();
        } 
        else
        {
            dirty();
        } 
    }
    
    /**
     * Returns a displayable string describing this <tt>Volume</tt>.
     * @return  The description
     */
    public String toString()
    {
        if (_alias != null) return "Volume(" + _alias + ")";
        else return "Volume(" + _pathName + ")";
    }
    
    int metaDataLength()
    {
        return _pathNameBytes.length + 20;
    }
    
    int writeMetaData(byte[] bytes, int offset)
    throws IOException
    {
        Util.putBytes(bytes, offset, IDENTIFIER_SIGNATURE);
        offset += IDENTIFIER_SIGNATURE.length;
        
        Util.putLong(bytes, offset, _id);
        offset += 8;
        
        Util.putChar(bytes, offset, _readOnly ? 1 : 0);
        offset += 2;
        
        Util.putChar(bytes, offset, _bufferSize);
        offset += 2;
        
        Util.putChar(bytes, offset, _pathNameBytes.length);
        offset += 2;
        
        Util.putBytes(bytes, offset, _pathNameBytes);
        offset += _pathNameBytes.length;
        
        return offset;
    }
    
    
    static int confirmMetaData(byte[] bytes, int offset)
    throws PersistitException
    {
        for (int i = 0; i < IDENTIFIER_SIGNATURE.length; i++)
        {
            if (offset > bytes.length ||
                IDENTIFIER_SIGNATURE[i] != bytes[offset++])
            {
                throw new CorruptVolumeException(
                    "Invalid Volume identifier signature");
            }
        }
        /*long id =*/ Util.getLong(bytes, offset);
        offset += 8;
        
        /*int flags =*/ Util.getChar(bytes, offset);
        offset += 2;
        
        /*int bufferSize =*/ Util.getChar(bytes, offset);
        offset += 2;
        
        int len = Util.getChar(bytes, offset);
        offset += 2;
        
        offset += len;
        return offset;
    }
    
    static long idFromMetaData(byte[] bytes, int offset)
    {
        return Util.getLong(bytes, offset + IDENTIFIER_SIGNATURE.length);
    }
    
    static int bufferSizeFromMetaData(byte[] bytes, int offset)
    {
        return Util.getChar(bytes, offset + IDENTIFIER_SIGNATURE.length + 10);
    }
    
    static String pathNameFromMetaData(byte[] bytes, int offset)
    {
        offset += IDENTIFIER_SIGNATURE.length + 12;
        int len = Util.getChar(bytes, offset);
        offset += 2;
        byte[] pathBytes = new byte[len];
        System.arraycopy(bytes, offset, pathBytes, 0, len);
        String pathName = new String(pathBytes);
        
        return pathName;
    }
    
    /**
     * Returns a hashCode that is invariant for this <tt>Volume</tt>.  It
     * is based on the volume's permanent ID value.  This method is used
     * by the prewrite journaling mechanism.
     * @return  The hash code
     */
    public int hashCode()
    {
        return ((int)(_id >>> 32) ^ (int)_id) & 0x7FFFFFFF;
    }
    
    /**
     * Implements equals.  This method is used by the prewrite journaling
     * mechanism.
     * @param o     The object to test for equality.
     * @return      <tt>true</tt> if the supplied object is equivalent to this
     *              <tt>Volume</tt>. 
     */
    public boolean equals(Object o)
    {
        if (o instanceof Volume)
        {
            Volume vol = (Volume)o;
            return (vol._id == _id) && (vol._pathName.equals(_pathName));
        }
        else return false;
    }
}
