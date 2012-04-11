/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.persistit;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.persistit.exception.InUseException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.ReadOnlyVolumeException;
import com.persistit.exception.TruncateVolumeException;
import com.persistit.exception.VolumeAlreadyExistsException;
import com.persistit.exception.VolumeClosedException;
import com.persistit.exception.VolumeNotFoundException;
import com.persistit.exception.WrongVolumeException;
import com.persistit.util.Util;

/**
 * <p>
 * Represent the identity and optionally the service classes that manage a
 * volume. A newly constructed Volume is "hollow" in the sense that it has no
 * ability to perform I/O on a backing file or allocate pages. In this state it
 * represents the identity, but not the content, of the volume.
 * </p>
 * <p>
 * To enable the <code>Volume</code> to act on data, you must supply a
 * {@link VolumeSpecification} object, either through the constructor or with
 * {@link #setSpecification}, and then call the {@link #open(Persistit)} method.
 * </p>
 * 
 * @author peter
 */
public class Volume {

    private final String _name;
    private long _id;
    private AtomicBoolean _closing = new AtomicBoolean();
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

    /**
     * Construct a hollow Volume with its specification.
     * 
     * @param specification
     */
    Volume(final VolumeSpecification specification) {
        this(specification.getName(), specification.getId());
        _specification = specification;
    }

    private void checkNull(final Object o, final String delegate) {
        if (o == null) {
            throw new IllegalStateException(this + " has no " + delegate);
        }
    }

    private void checkClosing() throws VolumeClosedException {
        if (_closing.get()) {
            throw new VolumeClosedException();
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

    void closing() {
        _closing.set(true);
    }

    /**
     * Release all resources for this <code>Volume</code> and invalidate all its
     * buffers in the {@link BufferPool}. Exchanges based on this
     * <code>Volume</code> may no longer be used after this call.
     * 
     * @throws PersistitException
     */
    public void close() throws PersistitException {
        closing();
        for (;;) {
            //
            // Prevents read/write operations from starting while the
            // volume is being closed.
            //
            final VolumeStorage storage = getStorage();
            if (!storage.claim(true)) {
                throw new InUseException("Unable to acquire claim on " + this);
            }
            try {
                //
                // BufferPool#invalidate may fail and return false if other
                // threads hold claims on pages of this volume. In that case we
                // need to back off all locks and retry
                //
                if (getStructure().getPool().invalidate(this)) {
                    getStructure().close();
                    getStorage().close();
                    getStatistics().reset();
                    break;
                }
            } finally {
                storage.release();
            }
            Util.sleep(Persistit.SHORT_DELAY);
        }
    }

    /**
     * Remove all data from this volume. This is equivalent to deleting and then
     * recreating the <code>Volume</code> except that it does not actually
     * delete and close the file. Instead, this method truncates the file,
     * rewrites the head page, and invalidates all buffers belonging to this
     * <code>Volume</code> in the {@link BufferPool}.
     * 
     * @throws PersistitException
     */
    public void truncate() throws PersistitException {
        if (isReadOnly()) {
            throw new ReadOnlyVolumeException();
        }
        if (!isTemporary() && !getSpecification().isCreate() && !getSpecification().isCreateOnly()) {
            throw new TruncateVolumeException();
        }
        for (;;) {
            //
            // Prevents read/write operations from starting while the
            // volume is being closed.
            //
            if (getStorage().claim(true, 0)) {
                try {
                    //
                    // BufferPool#invalidate may fail and return false if other
                    // threads hold claims on pages of this volume. In that case
                    // we
                    // need to back off all locks and retry
                    //
                    if (getStructure().getPool().invalidate(this)) {
                        getStructure().truncate();
                        getStorage().truncate();
                        getStatistics().reset();
                        break;
                    }
                } finally {
                    getStorage().release();
                }
            }
            Util.sleep(Persistit.SHORT_DELAY);
        }
    }

    /**
     * Delete the file backing this <code>Volume</code>.
     * 
     * @return <code>true</code> if the file existed and was successfully
     *         deleted.
     * @throws PersistitException
     */
    public boolean delete() throws PersistitException {
        if (!isClosed()) {
            throw new IllegalStateException("Volume must be closed before deletion");
        }
        return getStorage().delete();
    }

    /**
     * Returns the path name by which this volume was opened.
     * 
     * @return The path name
     */
    public String getPath() {
        return getStorage().getPath();
    }

    /**
     * @return page address of the first unused page in this <code>Volume</code>
     */
    public long getNextAvailablePage() {
        return getStorage().getNextAvailablePage();
    }

    /**
     * @return number of pages allocated to this <code>Volume</code>. Note that
     *         this method reflects the current length of the volume file, but
     *         on sparse file systems the disk space needed to store new pages
     *         may not yet have been allocated.
     */
    public long getExtendedPageCount() {
        return getStorage().getExtentedPageCount();
    }

    BufferPool getPool() {
        return getStructure().getPool();
    }

    Tree getDirectoryTree() {
        return getStructure().getDirectoryTree();
    }

    boolean isTemporary() { // TODO
        return getStorage().isTemp();
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
        checkClosing();
        return getStructure().getTree(name, createIfNecessary);
    }

    /**
     * Returns an array of all currently defined <code>Tree</code> names.
     * 
     * @return The array
     * 
     * @throws PersistitException
     */
    public String[] getTreeNames() throws PersistitException {
        checkClosing();
        return getStructure().getTreeNames();
    }

    /**
     * Return a TreeInfo structure for a tree by the specified name. If there is
     * no such tree, then return <code>null</code>.
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
     * Indicate whether this <code>Volume</code> has been opened or created,
     * i.e., whether a backing volume file has been created or opened.
     * 
     * @return <code>true</code> if there is a backing volume file.
     */
    public boolean isOpened() {
        return _storage != null && _storage.isOpened();
    }

    /**
     * Indicate whether this <code>Volume</code> has been closed.
     * 
     * @return <code>true</code> if this Volume is closed.
     */
    public boolean isClosed() {
        return _closing.get();
    }

    /**
     * Indicate whether this <code>Volume</code> prohibits updates.
     * 
     * @return <code>true</code> if this Volume prohibits updates.
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
    void open(final Persistit persistit) throws PersistitException {
        checkClosing();
        if (_specification == null) {
            throw new IllegalStateException("Missing VolumeSpecification");
        }
        if (_storage != null) {
            throw new IllegalStateException("This volume has already been opened");
        }
        final boolean exists = VolumeHeader.verifyVolumeHeader(_specification);

        _structure = new VolumeStructure(persistit, this, _specification.getPageSize());
        _storage = new VolumeStorageV2(persistit, this);
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

    void openTemporary(final Persistit persistit, final int pageSize) throws PersistitException {
        checkClosing();
        if (_storage != null) {
            throw new IllegalStateException("This volume has already been opened");
        }
        _structure = new VolumeStructure(persistit, this, pageSize);
        _storage = new VolumeStorageT2(persistit, this);
        _statistics = new VolumeStatistics();

        _storage.create();
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
     * @param appCache
     *            the object to be cached for application convenience.
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
            throw new IllegalStateException("Volume handle already set");
        }
        return handle;
    }

    /**
     * Resets the handle to zero. Intended for use only by tests.
     */
    void resetHandle() {
        _handle.set(0);
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
