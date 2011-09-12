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
import java.util.concurrent.atomic.AtomicReference;

import com.persistit.exception.PersistitException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeNotFoundException;
import com.persistit.exception.WrongVolumeException;

/**
 * Represent the identity and optionally the service classes that manage a
 * volume. A newly constructed Volume is "hollow" in the sense that it has no
 * ability to perform I/O on a backing file or allocate pages. In this state it
 * represents the identity, but not the content, of the volume.
 * <p />
 * To enable the <code>Volume</code> to act on data, you must supply a
 * {@link VolumeSpecification} object, either through the constructor or with
 * #setSpecification call either the {@link #open(Persistit)} method.
 * 
 * @author peter
 */
public class Volume {

    private final String _name;
    private long _id;

    private final AtomicInteger _handle = new AtomicInteger();
    private final AtomicReference<Object> _appCache = new AtomicReference<Object>();

    private VolumeSpecification _specification;
    private volatile VolumeStorage _storage;
    private volatile VolumeStatistics _statistics;
    private volatile VolumeStructure _structure;

    public static boolean isValidPageSize(final int pageSize) {
        for (int b = 1024; b <= 16384; b *= 2) {
            if (b == pageSize) {
                return true;
            }
        }
        return false;
    }

    /**
     * Construct a hollow Volume - used by JournalManager
     * 
     * @param name
     * @param id
     */
    Volume(final String name, final long id) {
        _name = name;
        _id = id;
    }

    Volume(final VolumeSpecification specification) {
        this(specification.getName(), specification.getId());
        _specification = specification;
    }

    private void checkNull(final Object o, final String delegate) {
        if (o == null) {
            throw new IllegalStateException(this + " has no " + delegate);
        }
    }

    VolumeSpecification getSpecification() {
        final VolumeSpecification s = _specification;
        checkNull(s, "VolumeSpecification");
        return s;
    }

    VolumeStorage getStorage() {
        final VolumeStorage s = _storage;
        checkNull(s, "VolumeStorage");
        return s;
    }

    VolumeStatistics getStatistics() {
        final VolumeStatistics s = _statistics;
        checkNull(s, "VolumeStatistics");
        return s;
    }

    VolumeStructure getStructure() {
        return _structure;
    }

    void setSpecification(final VolumeSpecification specification) {
        if (_specification != null) {
            throw new IllegalStateException("Volume " + this + " already has a VolumeSpecification");
        }
        if (specification.getName().equals(_name) && specification.getId() == _id) {
            _specification = specification;
        } else {
            throw new IllegalStateException("Volume " + this + " is incompatible with " + specification);
        }
    }

    /**
     * Returns the path name by which this volume was opened.
     * 
     * @return The path name
     */
    public String getPath() {
        return getStorage().getPath();
    }

    BufferPool getPool() {
        return getStructure().getPool();
    }

    Tree getDirectoryTree() {
        return getStructure().getDirectoryTree();
    }

    boolean isTemporary() { // TODO
        return false;
    }

    /**
     * @return The size in bytes of one page in this <code>Volume</code>.
     */
    public int getPageSize() {
        return getStructure().getPageSize();
    }

    /**
     * Looks up by name and returns a <code>NewTree</code> within this
     * <code>Volume</code>. If no such tree exists, this method either creates a
     * new tree or returns null depending on whether the
     * <code>createIfNecessary</code> parameter is <code>true</code>.
     * 
     * @param name
     *            The tree name
     * 
     * @param createIfNecessary
     *            Determines whether this method will create a new tree if there
     *            is no tree having the specified name.
     * 
     * @return The <code>NewTree</code>, or <code>null</code> if
     *         <code>createIfNecessary</code> is false and there is no such tree
     *         in this <code>Volume</code>.
     * 
     * @throws PersistitException
     */
    public Tree getTree(final String name, final boolean createIfNecessary) throws PersistitException {
        return getStructure().getTree(name, createIfNecessary);
    }

    /**
     * Returns an array of all currently defined <code>NewTree</code> names.
     * 
     * @return The array
     * 
     * @throws PersistitException
     */
    public String[] getTreeNames() throws PersistitException {
        return getStructure().getTreeNames();
    }

    /**
     * Return a TreeInfo structure for a tree by the specified name. If there is
     * no such tree, then return <i>null</i>.
     * 
     * @param tree
     *            name
     * @return an information structure for the Management interface.
     */
    Management.TreeInfo getTreeInfo(String name) {
        try {
            final Tree tree = getTree(name, false);
            if (tree != null) {
                return new Management.TreeInfo(tree);
            } else {
                return null;
            }
        } catch (PersistitException pe) {
            return null;
        }
    }

    /**
     * Indicates whether this <code>Volume</code> has been closed.
     * 
     * @return <i>true</i> if this Volume is closed.
     */
    public boolean isClosed() {
        return getStorage().isClosed();
    }

    /**
     * Indicates whether this <code>Volume</code> prohibits updates.
     * 
     * @return <i>true</i> if this Volume prohibits updates.
     */
    public boolean isReadOnly() {
        return getStorage().isReadOnly();
    }

    /**
     * Open an existing Volume file or create a new one, depending on the
     * settings of the {@link VolumeSpecification}.
     * 
     * @throws PersistitException
     */
    public void open(final Persistit persistit) throws PersistitException {
        if (_specification == null) {
            throw new IllegalStateException("Missing VolumeSpecification");
        }
        if (_storage != null) {
            throw new IllegalStateException("This volume has already been opened");
        }
        final boolean exists = VolumeHeader.verifyVolumeHeader(_specification);
        _structure = new VolumeStructure(persistit, this);
        _storage = new VolumeStorage(persistit, this);
        _statistics = new VolumeStatistics();

        if (exists) {
            if (_specification.isCreateOnly()) {
                throw new VolumeAlreadyExistsException(_specification.getPath());
            }
            _storage.open();
        } else {
            if (!_specification.isCreate()) {
                throw new VolumeNotFoundException(_specification.getPath());
            }
            _storage.create();
        }
        persistit.addVolume(this);
    }

    public String getName() {
        return _name;
    }

    public long getId() {
        return _id;
    }

    void setId(final long id) {
        if (id != 0 && _id != 0 && _id != id) {
            throw new IllegalStateException("Volume " + this + " already has id=" + _id);
        }
        _id = id;
    }

    void verifyId(final long id) throws WrongVolumeException {
        if (id != 0 && _id != 0 && id != _id) {
            throw new WrongVolumeException(this + "id " + _id + " does not match expected id " + id);
        }
    }

    /**
     * Store an Object with this Volume for the convenience of an application.
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
        if (handle == 0) {
            _handle.set(0);
        } else if (!_handle.compareAndSet(0, handle)) {
            throw new IllegalStateException("Tree handle already set");
        }
        return handle;
    }

    @Override
    public String toString() {
        VolumeSpecification specification = _specification;
        if (specification != null) {
            return specification.summary();
        } else {
            return _name;
        }
    }

    @Override
    public int hashCode() {
        return _name.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Volume) {
            final Volume volume = (Volume) o;
            return volume.getName().equals(getName()) && volume.getId() == getId();
        }
        return false;
    }
}
