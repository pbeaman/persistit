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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.persistit.exception.CorruptVolumeException;
import com.persistit.exception.PersistitException;
import com.persistit.util.Debug;
import com.persistit.util.Util;

/**
 * Represents a single B-Tree within a {@link Volume}.
 */
public class Tree extends SharedResource {
    final static int MAX_SERIALIZED_SIZE = 512;
    final static int MAX_TREE_NAME_SIZE = 256;

    private final String _name;
    private final Volume _volume;
    private volatile long _rootPageAddr;
    private volatile int _depth;
    private AtomicLong _changeCount = new AtomicLong(-1);
    private AtomicReference<Object> _appCache = new AtomicReference<Object>();
    private AtomicInteger _handle = new AtomicInteger();

    private final TreeStatistics _treeStatistics = new TreeStatistics();

    Tree(final Persistit persistit, Volume volume, String name) {
        super(persistit);
        int serializedLength = name.getBytes().length;
        if (serializedLength > MAX_TREE_NAME_SIZE) {
            throw new IllegalArgumentException("Tree name too long: " + name.length() + "(as " + serializedLength
                    + " bytes)");
        }
        _name = name;
        _volume = volume;
        _generation.set(1);
    }

    public Volume getVolume() {
        return _volume;
    }

    public String getName() {
        return _name;
    }

    // TODO - renameTree
    @Override
    public int hashCode() {
        return _volume.hashCode() ^ _name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Tree) {
            Tree tree = (Tree) o;
            return _name.equals(tree._name) && _volume.equals(tree.getVolume());
        } else {
            return false;
        }
    }

    /**
     * Returns the page address of the root page of this <code>Tree</code>. The
     * root page will be a data page if the <code>Tree</code> has only one page,
     * or will be the top index page of the B-Tree.
     * 
     * @return The page address
     */
    public long getRootPageAddr() {
        return _rootPageAddr;
    }

    /**
     * @return the number of levels of the <code>Tree</code>.
     */
    public int getDepth() {
        return _depth;
    }

    void changeRootPageAddr(long rootPageAddr, int deltaDepth) throws PersistitException {
        Debug.$assert0.t(isMine());
        _rootPageAddr = rootPageAddr;
        _depth += deltaDepth;
    }

    void bumpChangeCount() {
        //
        // Note: the changeCount only gets written when there's a structure
        // change in the tree that causes it to be committed.
        //
        _changeCount.incrementAndGet();
    }

    /**
     * @return The number of key-value insert/delete operations performed on
     *         this tree; does not including replacement of an existing value
     */
    long getChangeCount() {
        return _changeCount.get();
    }

    /**
     * Save a Tree in the directory
     * 
     * @param value
     */
    int store(final byte[] bytes, final int index) {
        byte[] nameBytes = Util.stringToBytes(_name);
        Util.putLong(bytes, index, _rootPageAddr);
        Util.putLong(bytes, index + 8, getChangeCount());
        Util.putShort(bytes, index + 16, _depth);
        Util.putShort(bytes, index + 18, nameBytes.length);
        Util.putBytes(bytes, index + 20, nameBytes);
        return 20 + nameBytes.length;
    }

    /**
     * Load an existing Tree from the directory
     * 
     * @param value
     */
    int load(final byte[] bytes, final int index, final int length) {
        int nameLength = length < 20 ? -1 : Util.getShort(bytes, index + 18);
        if (nameLength < 1 || nameLength + 20 > length) {
            throw new IllegalStateException("Invalid tree record is too short for tree " + _name + ": " + length);
        }
        final String name = new String(bytes, index + 20, nameLength);
        if (!_name.equals(name)) {
            throw new IllegalStateException("Invalid tree name recorded: " + name + " for tree " + _name);
        }
        _rootPageAddr = Util.getLong(bytes, index);
        _changeCount.set(Util.getLong(bytes, index + 8));
        _depth = Util.getShort(bytes, index + 16);
        return length;
    }

    /**
     * Initialize a Tree.
     * 
     * @param rootPageAddr
     * @throws PersistitException
     */
    void setRootPageAddress(final long rootPageAddr) throws PersistitException {
        if (_rootPageAddr != rootPageAddr) {
            // Derive the index depth
            Buffer buffer = null;
            try {
                buffer = getVolume().getStructure().getPool().get(_volume, rootPageAddr, false, true);
                int type = buffer.getPageType();
                if (type < Buffer.PAGE_TYPE_DATA || type > Buffer.PAGE_TYPE_INDEX_MAX) {
                    throw new CorruptVolumeException(String.format("Tree root page %,d has invalid type %s",
                            rootPageAddr, buffer.getPageTypeName()));
                }
                _rootPageAddr = rootPageAddr;
                _depth = type - Buffer.PAGE_TYPE_DATA + 1;
            } finally {
                if (buffer != null) {
                    buffer.releaseTouched();
                }
            }
        }
    }

    /**
     * Invoked when this <code>Tree</code> is being deleted. This causes
     * subsequent operations by any <code>Exchange</code>s on this
     * <code>Tree</code> to fail.
     */
    void invalidate() {
        super.clearValid();
        _depth = -1;
        _rootPageAddr = -1;
        _generation.set(-1);
    }

    public TreeStatistics getStatistics() {
        return _treeStatistics;
    }

    /**
     * Returns a displayable description of the <code>Tree</code>, including its
     * name, its internal tree index, its root page address, and its depth.
     * 
     * @return A displayable summary
     */
    @Override
    public String toString() {
        return "<Tree " + _name + " rootPageAddr=" + _rootPageAddr + " depth=" + _depth + " status="
                + getStatusDisplayString() + ">";
    }

    /**
     * Store an Object with this Tree for the convenience of an application.
     * 
     * @param the
     *            object to be cached for application convenience.
     */
    public void setAppCache(Object appCache) {
        _appCache.set(appCache);
    }

    /**
     * @return the object cached for application convenience
     */
    public Object getAppCache() {
        return _appCache.get();
    }

    /**
     * @return The handle value used to identify this Tree in the journal
     */
    public int getHandle() {
        return _handle.get();
    }

    /**
     * Set the handle used to identify this Tree in the journal. May be invoked
     * only once.
     * 
     * @param handle
     * @return
     * @throws IllegalStateException
     *             if the handle has already been set
     */
    int setHandle(final int handle) {
        if (!_handle.compareAndSet(0, handle)) {
            throw new IllegalStateException("Tree handle already set");
        }
        return handle;
    }

}
