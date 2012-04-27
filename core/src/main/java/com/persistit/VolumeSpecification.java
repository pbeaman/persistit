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

/**
 * 
 */
package com.persistit;

import java.io.File;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import com.persistit.exception.InvalidVolumeSpecificationException;
import com.persistit.exception.VolumeAlreadyExistsException;

/**
 * A structure holding all the specification details for a {@link Volume}.
 * 
 * @author peter
 * 
 */
public class VolumeSpecification {

    private final static String ATTR_NAME = "name";
    private final static String ATTR_ALIAS = "alias";
    private final static String ATTR_CREATE = "create";
    private final static String ATTR_READONLY = "readOnly";
    private final static String ATTR_CREATEONLY = "createOnly";
    private final static String ATTR_PAGE_SIZE = "pageSize";

    private final static String ATTR_INITIAL_SIZE = "initialSize";
    private final static String ATTR_EXTENSION_SIZE = "extensionSize";
    private final static String ATTR_MAXIMUM_SIZE = "maximumSize";

    private final static String ATTR_INITIAL_PAGES = "initialPages";
    private final static String ATTR_EXTENSION_PAGES = "extensionPages";
    private final static String ATTR_MAXIMUM_PAGES = "maximumPages";

    private String path;
    private String name = null;
    private boolean readOnly = false;
    private boolean create = false;
    private boolean createOnly = false;

    private int pageSize = -1;
    private int version = -1;
    private long id = 0;

    private long initialPages = -1;
    private long extensionPages = -1;
    private long maximumPages = -1;
    private long initialSize = -1;
    private long extensionSize = -1;
    private long maximumSize = -1;

    public static String nameFromFile(final File file) {
        String name = file.getName();
        int p = name.lastIndexOf('.');
        return p < 1 ? name : name.substring(0, p);
    }

    /**
     * Construct a <code>VolumeSpecification</code> having the supplied
     * parameter values.
     * 
     * @param path
     *            File system path to volume file
     * @param name
     *            Name of volume. If <code>null</code> the file name in the
     *            specified <code>path</code> is used.
     * @param pageSize
     *            Page size: one of 1024, 2048, 4096, 8192 or 6384
     * @param initialPages
     *            Initial page count
     * @param maximumPages
     *            Maximum page count
     * @param extensionPages
     *            Number of pages to extend by when file must grow
     * @param create
     *            <code>true</code> to allow creation of a new volume
     * @param createOnly
     *            <code>true</code> to require creation of a new volume
     * @param readOnly
     *            <code>true</code> to open volume file in read-only mode
     */
    public VolumeSpecification(final String path, final String name, final int pageSize, final long initialPages,
            final long maximumPages, final long extensionPages, final boolean create, final boolean createOnly,
            final boolean readOnly) {
        this.path = path;
        this.name = name == null ? nameFromFile(new File(path)) : name;
        this.pageSize = pageSize;
        this.initialPages = initialPages;
        this.maximumPages = maximumPages;
        this.extensionPages = extensionPages;
        this.create = create;
        this.createOnly = createOnly;
        this.readOnly = readOnly;
    }

    /**
     * Construct a <code>VolumeSpecification</code> from the supplied
     * specification string. The specification has the form: <br />
     * <i>pathname</i>[,<i>options</i>]... <br />
     * where options include: <br />
     * <dl>
     * <dt><code>name</code></dt>
     * <dd>The Volume name used in looking up the volume within Persistit
     * programs (see {@link com.persistit.Persistit#getVolume(String)}). If the
     * name attribute is not specified, the last name in the Volume's path name
     * sequence is used instead.</dd>
     * 
     * <dt><code>readOnly</code></dt>
     * <dd>Open in Read-Only mode. (Incompatible with create mode.)</dd>
     * 
     * <dt><code>create</code></dt>
     * <dd>Creates the volume if it does not exist. Requires
     * <code>bufferSize</code>, <code>initialPagesM</code>,
     * <code>extensionPages</code> and <code>maximumPages</code> to be
     * specified.</dd>
     * 
     * <dt><code>createOnly</code></dt>
     * <dd>Creates the volume, or throw a {@link VolumeAlreadyExistsException}
     * if it already exists.</dd>
     * 
     * <dt><code>temporary</code></dt>
     * <dd>Creates the a new, empty volume regardless of whether an existing
     * volume file already exists.</dd>
     * 
     * <dt><code>bufferSize:<i>NNN</i></code></dt>
     * <dd>Specifies <i>NNN</i> as the volume's buffer size when creating a new
     * volume. <i>NNN</i> must be 1024, 2048, 4096, 8192 or 16384.</dd>
     * 
     * <dt><code>initialPages:<i>NNN</i></code></dt>
     * <dd><i>NNN</i> is the initial number of pages to be allocated when this
     * volume is first created.</dd>
     * 
     * <dt><code>extensionPages:<i>NNN</i></code></dt>
     * <dd><i>NNN</i> is the number of pages by which to extend the volume when
     * more pages are required.</dd>
     * 
     * <dt><code>maximumPages:<i>NNN</i></code></dt>
     * <dd><i>NNN</i> is the maximum number of pages to which this volume can
     * extend.</dd>
     * 
     * </dl>
     * <p>
     * 
     * @param specification
     *            the specification String
     * @throws InvalidVolumeSpecificationException
     */

    public VolumeSpecification(final String specification) throws InvalidVolumeSpecificationException {
        StringTokenizer mainTokenizer = new StringTokenizer(specification, ",");
        try {
            path = mainTokenizer.nextToken().trim();
            name = nameFromFile(new File(path));

            while (mainTokenizer.hasMoreTokens()) {
                String token = mainTokenizer.nextToken().trim();
                StringTokenizer innerTokenizer = new StringTokenizer(token, ":");
                String attr = innerTokenizer.nextToken().trim();
                if (ATTR_READONLY.equals(attr)) {
                    readOnly = true;
                } else if (ATTR_CREATE.equals(attr)) {
                    create = true;
                } else if (ATTR_CREATEONLY.equals(attr)) {
                    createOnly = true;
                } else if (ATTR_NAME.equals(attr) || ATTR_ALIAS.equals(attr)) {
                    String valueString = innerTokenizer.nextToken().trim();
                    if (valueString != null && !valueString.isEmpty()) {
                        name = valueString;
                    }
                } else {
                    String valueString = innerTokenizer.nextToken().trim();
                    boolean bad = false;
                    long value = Configuration.parseLongProperty(attr, valueString);

                    if (ATTR_PAGE_SIZE.equals(attr)) {
                        if (value < Integer.MAX_VALUE && value > 0 && Volume.isValidPageSize((int) value)) {
                            pageSize = (int) value;
                        } else {
                            throw new InvalidVolumeSpecificationException("Invalid pageSize " + specification);
                        }
                    } else if (ATTR_INITIAL_PAGES.equals(attr)) {
                        initialPages = value;
                    } else if (ATTR_EXTENSION_PAGES.equals(attr)) {
                        extensionPages = value;
                    } else if (ATTR_MAXIMUM_PAGES.equals(attr)) {
                        maximumPages = value;
                    } else if (ATTR_INITIAL_SIZE.equals(attr)) {
                        initialSize = value;
                    } else if (ATTR_EXTENSION_SIZE.equals(attr)) {
                        extensionSize = value;
                    } else if (ATTR_MAXIMUM_SIZE.equals(attr)) {
                        maximumSize = value;
                    } else {
                        bad = true;
                    }

                    if (bad || innerTokenizer.hasMoreTokens()) {
                        throw new InvalidVolumeSpecificationException("Unknown attribute " + attr + " in "
                                + specification);
                    }
                }
            }
            int n = 0;
            if (readOnly) {
                n++;
            }
            if (create) {
                n++;
            }
            if (createOnly) {
                n++;
            }
            if (n > 1) {
                throw new InvalidVolumeSpecificationException(specification + ": readOnly, create and createOnly "
                        + "attributes are mutually exclusive");
            }
            //
            // Allows size specification in bytes rather than pages.
            //
            if (pageSize > 0) {
                if (initialPages == -1 && initialSize > 0) {
                    initialPages = (initialSize + (pageSize - 1)) / pageSize;
                }
                if (extensionPages == -1 && extensionSize > 0) {
                    extensionPages = (extensionSize + (pageSize - 1)) / pageSize;
                }
                if (maximumPages == -1 && maximumSize > 0) {
                    maximumPages = (maximumSize + (pageSize - 1)) / pageSize;
                }
            }

            // Validate initial, maximum and extension sizes
            if (maximumPages <= 0) {
                maximumPages = initialPages;
            }

            if (create || createOnly) {
                if (initialPages < 1 || initialPages > Long.MAX_VALUE / pageSize) {
                    throw new InvalidVolumeSpecificationException("Invalid initial page count: " + initialPages);
                }

                if (extensionPages < 0 || extensionPages > Long.MAX_VALUE / pageSize) {
                    throw new InvalidVolumeSpecificationException("Invalid extension page count: " + extensionPages);
                }

                if (maximumPages < initialPages || maximumPages > Long.MAX_VALUE / pageSize) {
                    throw new InvalidVolumeSpecificationException("Invalid maximum page count: " + maximumPages);
                }
            }
        } catch (NumberFormatException nfe) {
            throw new InvalidVolumeSpecificationException(specification + ": invalid number");
        } catch (NoSuchElementException nste) {
            throw new InvalidVolumeSpecificationException(specification + ": " + nste);
        }
    }

    public void setPageSize(final int value) throws InvalidVolumeSpecificationException {
        if (!Volume.isValidPageSize(value)) {
            throw new InvalidVolumeSpecificationException("Invalid pageSize " + value);
        }

        if (pageSize == value || pageSize == -1) {
            pageSize = value;
        } else {
            throw new InvalidVolumeSpecificationException("Mismatched volume pageSize " + value + " for " + this);
        }
    }

    public void setVersion(final int value) throws InvalidVolumeSpecificationException {
        if (version == value || version == -1) {
            version = value;
        } else {
            throw new InvalidVolumeSpecificationException("Mismatched volume version " + value + " for " + this);
        }
    }

    public void setId(final long value) throws InvalidVolumeSpecificationException {
        if (id == value || id == 0) {
            id = value;
        } else {
            throw new InvalidVolumeSpecificationException("Mismatched volume version " + value + " for " + this);
        }
    }

    public String getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isCreate() {
        return create || createOnly;
    }

    public boolean isCreateOnly() {
        return createOnly;
    }

    public int getPageSize() {
        return pageSize;
    }

    public long getInitialPages() {
        return initialPages;
    }

    public long getExtensionPages() {
        return extensionPages;
    }

    public long getMaximumPages() {
        return maximumPages;
    }

    public long getInitialSize() {
        return initialSize;
    }

    public long getExtensionSize() {
        return extensionSize;
    }

    public long getMaximumSize() {
        return maximumSize;
    }

    public int getVersion() {
        return version;
    }

    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(path);
        sb.append(',').append(ATTR_NAME).append(':').append(name);
        sb.append(',').append(ATTR_PAGE_SIZE).append(':').append(pageSize);

        if (initialPages >= 0) {
            sb.append(',').append(ATTR_INITIAL_SIZE).append(':').append(ds(initialPages));
        }
        if (maximumPages >= 0) {
            sb.append(',').append(ATTR_MAXIMUM_SIZE).append(':').append(ds(maximumPages));
        }
        if (extensionPages >= 0) {
            sb.append(',').append(ATTR_EXTENSION_SIZE).append(':').append(ds(extensionPages));
        }
        if (readOnly) {
            sb.append(',').append(ATTR_READONLY);
        }
        if (createOnly) {
            sb.append(',').append(ATTR_CREATEONLY);
        } else if (create) {
            sb.append(',').append(ATTR_CREATE);
        }
        return sb.toString();
    }

    private String ds(final long pages) {
        return Configuration.displayableLongValue(pages * pageSize);
    }

    public String summary() {
        return name + "(" + path + ")";
    }

    /**
     * @param initialPages
     *            the initialPages to set
     */
    void setInitialPages(long initialPages) {
        this.initialPages = initialPages;
    }

    /**
     * @param extensionPages
     *            the extensionPages to set
     */
    void setExtensionPages(long extensionPages) {
        this.extensionPages = extensionPages;
    }

    /**
     * @param maximumPages
     *            the maximumPages to set
     */
    void setMaximumPages(long maximumPages) {
        this.maximumPages = maximumPages;
    }

}