package com.persistit;

import java.nio.charset.Charset;

/**
 * 
 * All multi-byte integers are stored in big-endian form. General record format:
 * 
 * <table border="1">
 * <tr valign="top">
 * <td>+0</td>
 * <td>length</td>
 * </tr>
 * <tr valign="top">
 * <td>+4</td>
 * <td>type</td>
 * </tr>
 * <tr valign="top">
 * <td>+8</td>
 * <td>timestamp</td>
 * </tr>
 * <tr valign="top">
 * <td>+16</td>
 * <td>payload</td>
 * </tr>
 * </table>
 * <p />
 * Type: two ASCII bytes:
 * <p />
 * <table border="1">
 * <tr valign="top">
 * <td>IV</td>
 * <td>Identify Volume: associates an integer handle to a Volume. This handle is
 * referenced by subsequent log records to identify this Volume. The handle has
 * no meaning beyond the scope of one log file; every new log generation gets
 * new IV records.
 * <table>
 * <tr valign="top">
 * <td>+16</td>
 * <td>Volume handle (int)</td>
 * </tr>
 * <tr valign="top">
 * <td>+20</td>
 * <td>Volume Id (long></td>
 * </tr>
 * <tr valign="top">
 * <td>+28</td>
 * <td>Volume Path (variable - length determined by record length)</td>
 * </tr>
 * </table>
 * </td>
 * </tr>
 * <tr valign="top">
 * <td>IT</td>
 * <td>Identify Tree: associates an integer handle to a Tree. This handle is
 * referenced by subsequent log records to identify this Tree. The handle has no
 * meaning beyond the scope of one log file; every new log generation gets new
 * IT records.
 * <table>
 * <tr valign="top">
 * <td>+16</td>
 * <td>Tree handle (int)</td>
 * </tr>
 * <tr valign="top">
 * <td>+20</td>
 * <td>Volume handle</td>
 * </tr>
 * <tr valign="top">
 * <td>+24</td>
 * <td>Tree Name (variable - length determined by record length)</td>
 * </tr>
 * </table>
 * </td>
 * </tr>
 * <tr valign="top">
 * <td>PA</td>
 * <td>Page Image
 * <table>
 * <tr valign="top">
 * <td>+16</td>
 * <td>Volume handle (int) - refers to a volume defined in a preceding IV record
 * </td>
 * </tr>
 * <tr valign="top">
 * <td>+20</td>
 * <td>page address (long)</td>
 * </tr>
 * <tr valign="top">
 * <td>+28</td>
 * <td>leftSize (int)</td>
 * </tr>
 * <tr valign="top">
 * <td>+32</td>
 * <td>bytes: the first leftSize bytes will go into the page at offset 0 the
 * remaining bytes will go to the end of the page; the middle of the page will
 * be cleared</td>
 * </tr>
 * </table>
 * </td>
 * </tr>
 * <tr valign="top">
 * <td>CP</td>
 * <td>Checkpoint. Specifies a timestamp and a system time in millis at which
 * all pages modified prior to that timestamp are present in the log.
 * <table>
 * <tr valign="top">
 * <td>+16</td>
 * <td>System time in milliseconds (long)</td>
 * </tr>
 * </table>
 * </td>
 * </tr>
 * <tr valign="top">
 * <td>TS</td>
 * <td>Transaction Start: binds subsequent records having same timestamp to a
 * commit</td>
 * </tr>
 * <tr valign="top">
 * <td>TJ</td>
 * <td>Transaction timestamp Join
 * <table>
 * <tr valign="top">
 * <td>+16</td>
 * <td>previous timestamp: subsequent records having this record's timestamp are
 * members of the same transaction as previous timestamp.</td>
 * </tr>
 * </table>
 * </td>
 * </tr>
 * <tr valign="top">
 * <td>TC</td>
 * <td>Transaction Commit: all records having same or linked time-stamps should
 * be applied</td>
 * </tr>
 * <tr valign="top">
 * <td>TR</td>
 * <td>Transaction Roll-back: ignore/roll-back: all records having same or
 * linked time-stamps should be ignored</td>
 * </tr>
 * <tr valign="top">
 * <td>WR</td>
 * <td>Write Record - specifies a Tree into which a key/value pair should be
 * inserted
 * <table>
 * <tr valign="top">
 * <td>+16</td>
 * <td>Tree handle (int) - matches a tree identified in a preceding IT record</td>
 * </tr>
 * <tr valign="top">
 * <td>+20</td>
 * <td>Key size - 2-byte little-endian</td>
 * </tr>
 * <tr valign="top">
 * <td>+22</td>
 * <td>Key bytes immediately followed by Value bytes (variable).</td>
 * </tr>
 * </table>
 * </dt>
 * </tr>
 * <tr valign="top">
 * <td>DV</td>
 * <td>Delete Volume - specifies a Volume to be deleted.
 * <table>
 * <tr valign="top">
 * <td>+16</td>
 * <td>Volume handle (int) - matches a volume identified in a preceding IV
 * record</td>
 * </tr>
 * </table>
 * </dt>
 * </tr>
 * <tr valign="top">
 * <td>DT</td>
 * <td>Delete Tree - specifies a Tree to be deleted.
 * <table>
 * <tr valign="top">
 * <td>+16</td>
 * <td>Tree handle (int) - matches a tree identified in a preceding IT record</td>
 * </tr>
 * </table>
 * </dt>
 * </tr>
 * <tr valign="top">
 * <td>DR</td>
 * <td>Delete Record - specifies a Tree and two Keys: all key/value pairs
 * between these two keys (inclusive) are deleted.
 * <table>
 * <tr valign="top">
 * <td>+16</td>
 * <td>Tree handle (int) - matches a tree identified in a preceding IT record</td>
 * </tr>
 * <tr valign="top">
 * <td>+20</td>
 * <td>Key1_size - 2-byte little-endian</td>
 * </tr>
 * <tr valign="top">
 * </tr>
 * <tr valign="top">
 * <td>+20</td>
 * <td>Key2 Elision_count - 2-byte little-endian</td>
 * </tr>
 * <tr valign="top">
 * <td>+24</td>
 * <td>Key bytes define two keys, key1 and key2. These delimit the range to be
 * deleted. The first Key1_size bytes of this field contain the encoded key1
 * value. The remaining bytes define key2. However, the first Elision_count
 * bytes of key2 are the same as key1, therefore only.</td>
 * </tr>
 * </table>
 * </dt>
 * </tr>
 * 
 * </table>
 * 
 * @author peter
 * 
 */
public class LogRecord {

    private final static Charset UTF8 = Charset.forName("UTF-8");

    public final static int TYPE_IV = ('I' << 8) | 'V';

    public final static int TYPE_IT = ('I' << 8) | 'T';

    public final static int TYPE_PA = ('P' << 8) | 'A';

    public final static int TYPE_WR = ('W' << 8) | 'R';

    public final static int TYPE_RR = ('R' << 8) | 'R';

    public final static int TYPE_TS = ('T' << 8) | 'S';

    public final static int TYPE_TC = ('T' << 8) | 'C';

    public final static int TYPE_TJ = ('T' << 8) | 'J';

    public final static int TYPE_CP = ('C' << 8) | 'P';

    public final static int OVERHEAD = 16;

    public static int getLength(final byte[] bytes) {
        return Util.getInt(bytes, 0);
    }

    public static void putLength(final byte[] bytes, int length) {
        Util.putInt(bytes, 0, length);
    }

    public static int getType(final byte[] bytes) {
        return Util.getChar(bytes, 4);
    }

    public static void putType(final byte[] bytes, int type) {
        Util.putChar(bytes, 4, type);
    }

    public static long getTimestamp(final byte[] bytes) {
        return Util.getLong(bytes, 8);
    }

    public static void putTimestamp(final byte[] bytes, long timestamp) {
        Util.putLong(bytes, 8, timestamp);
    }

    public static class IV extends LogRecord {

        public static int MAX_LENGTH = 24 + 1024;

        public static void putType(final byte[] bytes) {
            putType(bytes, TYPE_IV);
        }

        public static int getHandle(final byte[] bytes) {
            return Util.getInt(bytes, 16);
        }

        public static void putHandle(final byte[] bytes, final int handle) {
            Util.putInt(bytes, 16, handle);
        }

        public static long getVolumeId(final byte[] bytes) {
            return Util.getLong(bytes, 20);
        }

        public static void putVolumeId(final byte[] bytes, final long volumeId) {
            Util.putLong(bytes, 20, volumeId);
        }

        public static String getVolumeName(final byte[] bytes) {
            final int length = getLength(bytes) - 28;
            return new String(bytes, 28, length, UTF8);
        }

        public static void putVolumeName(final byte[] bytes,
                final String volumeName) {
            final byte[] stringBytes = volumeName.getBytes(UTF8);
            System.arraycopy(stringBytes, 0, bytes, 28, stringBytes.length);
            putLength(bytes, 28 + stringBytes.length);
        }
    }

    public static class IT extends LogRecord {

        public static int MAX_LENGTH = 24 + 1024;

        public static void putType(final byte[] bytes) {
            putType(bytes, TYPE_IV);
        }

        public static int getHandle(final byte[] bytes) {
            return Util.getInt(bytes, 16);
        }

        public static void putHandle(final byte[] bytes, final int handle) {
            Util.putInt(bytes, 16, handle);
        }

        public static int getVolumeHandle(final byte[] bytes) {
            return Util.getInt(bytes, 20);
        }

        public static void putVolumeHandle(final byte[] bytes,
                final int volumeHandle) {
            Util.putInt(bytes, 20, volumeHandle);
        }

        public static String getTreeName(final byte[] bytes) {
            final int length = getLength(bytes) - 24;
            return new String(bytes, 24, length, UTF8);
        }

        public static void putTreeName(final byte[] bytes, final String treeName) {
            final byte[] stringBytes = treeName.getBytes(UTF8);
            System.arraycopy(stringBytes, 0, bytes, 24, stringBytes.length);
            putLength(bytes, 24 + stringBytes.length);
        }
    }

    public static class PA extends LogRecord {

        public final static int OVERHEAD = 36;

        public final static int TYPE = TYPE_PA;

        public static void putType(final byte[] bytes) {
            putType(bytes, TYPE_PA);
        }

        public static int getVolumeHandle(final byte[] bytes) {
            return Util.getInt(bytes, 16);
        }

        public static void putVolumeHandle(final byte[] bytes,
                final int volumeHandle) {
            Util.putInt(bytes, 16, volumeHandle);
        }

        public static long getPageAddress(final byte[] bytes) {
            return Util.getLong(bytes, 20);
        }

        public static void putPageAddress(final byte[] bytes,
                final long pageAddress) {
            Util.putLong(bytes, 20, pageAddress);
        }

        public static int getLeftSize(final byte[] bytes) {
            return Util.getInt(bytes, 28);
        }

        public static void putLeftSize(final byte[] bytes, final int leftSize) {
            Util.putInt(bytes, 28, leftSize);
        }

        public static int getBufferSize(final byte[] bytes) {
            return Util.getInt(bytes, 32);
        }

        public static void putBufferSize(final byte[] bytes,
                final int bufferSize) {
            Util.putInt(bytes, 32, (char) bufferSize);
        }

    }

    public static class CP extends LogRecord {

        public final static int OVERHEAD = 24;

        public final static int TYPE = TYPE_CP;

        public static void putType(final byte[] bytes) {
            putType(bytes, TYPE_PA);
        }

        public static long getSystemTimeMillis(final byte[] bytes) {
            return Util.getLong(bytes, 16);
        }

        public static void putSystemTimeMillis(final byte[] bytes,
                final long systemTimeMillis) {
            Util.putLong(bytes, 16, systemTimeMillis);
        }

    }
}
