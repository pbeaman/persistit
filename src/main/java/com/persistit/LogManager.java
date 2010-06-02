package com.persistit;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Pattern;

import com.persistit.exception.CorruptLogException;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitIOException;

/**
 * Manages the disk-based I/O log. The log contains both committed transactions
 * and images of updated pages.
 * 
 * @author peter
 * 
 */
public class LogManager {

	private final static long GIG = 1024 * 1024 * 1024;

	public final static long DEFAULT_LOG_SIZE = GIG;

	public final static long MINIMUM_LOG_SIZE = GIG / 64;

	public final static long MAXIMUM_LOG_SIZE = GIG * 64;

	public final static int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;

	public final static int DEFAULT_READ_BUFFER_SIZE = 32 * 1024;

	public final static long DEFAULT_FLUSH_INTERVAL = 100;

	public final static int MAXIMUM_MAPPED_HANDLES = 4096;

	private final static String PATH_FORMAT = "%s.%06d";

	private final static String PATH_PATTERN = ".+\\.\\d{6}";

	private final Map<VolumePage, FileAddress> _pageMap = new HashMap<VolumePage, FileAddress>();

	private final Map<File, FileChannel> _fileChannelMap = new HashMap<File, FileChannel>();

	private final Map<VolumeDescriptor, Integer> _volumeToHandleMap = new HashMap<VolumeDescriptor, Integer>();

	private final Map<Integer, VolumeDescriptor> _handleToVolumeMap = new HashMap<Integer, VolumeDescriptor>();

	private final Map<Tree, Integer> _treeToHandleMap = new HashMap<Tree, Integer>();

	private final Map<Integer, Tree> _handleToTreeMap = new HashMap<Integer, Tree>();

	private File _currentFile;

	private FileChannel _currentChannel;

	private long _maximumFileSize;

	private Persistit _persistit;

	private int _writeBufferSize = DEFAULT_BUFFER_SIZE;

	private MappedByteBuffer _writeBuffer;

	private long _writeBufferAddress = 0;

	private int _readBufferSize = DEFAULT_READ_BUFFER_SIZE;

	private ByteBuffer _readBuffer;

	private long _flushInterval = DEFAULT_FLUSH_INTERVAL;

	private Timer _flushTimer;

	private File _directory;

	private final byte[] _bytes = new byte[4096];

	/**
	 * Log file generation - serves as suffix on file name
	 */
	private int _generation;

	private int _handleCounter = 0;

	private boolean _recovered;

	private class LogFlusher extends TimerTask {
		@Override
		public void run() {
			try {
				force();
			} catch (PersistitIOException ioe) {
				ioe.printStackTrace();
			}
		}
	}

	private static class LogNotClosedException extends Exception {

	}

	public LogManager(final Persistit persistit) {
		_persistit = persistit;
	}

	public void init(final String path, final long maximumSize) {
		_directory = new File(path).getAbsoluteFile();
		_maximumFileSize = maximumSize;
		_readBuffer = ByteBuffer.allocateDirect(_readBufferSize);
		_flushTimer = new Timer("LOG_FLUSHER", true);
		_flushTimer.schedule(new LogFlusher(), _flushInterval, _flushInterval);
	}

	public synchronized boolean readPageFromLog(final Buffer buffer)
			throws PersistitIOException {
		final Volume volume = buffer.getVolume();
		final VolumePage vp = new VolumePage(new VolumeDescriptor(volume),
				buffer.getPageAddress());
		final FileAddress fa = _pageMap.get(vp);
		if (fa == null) {
			return false;
		}
		final FileChannel fc = getFileChannel(fa.getFile());
		final ByteBuffer bb = buffer.getByteBuffer();
		try {
			_readBuffer.position(0).limit(
					LogRecord.PA.OVERHEAD + buffer.getBufferSize());
			fc.read(_readBuffer, fa.getAddress());
			_readBuffer.flip();
			_readBuffer.get(_bytes, 0, LogRecord.PA.OVERHEAD);
			final char type = LogRecord.PA.getType(_bytes);
			final int payloadSize = LogRecord.PA.getLength(_bytes)
					- LogRecord.PA.OVERHEAD;
			final int leftSize = LogRecord.PA.getLeftSize(_bytes);
			final long pageAddress = LogRecord.PA.getPageAddress(_bytes);

			if (type != LogRecord.PA.TYPE) {
				throw new CorruptLogException("Record at " + fa + " is invalid");
			}

			if (pageAddress != buffer.getPageAddress()) {
				throw new CorruptLogException("Record at " + fa
						+ " is not volume/page " + vp);
			}

			if (leftSize < 0 || payloadSize < leftSize
					|| payloadSize > buffer.getBufferSize()) {
				throw new CorruptLogException("Record at " + fa
						+ " invalid sizes: recordSize= " + payloadSize
						+ " leftSize=" + leftSize + " bufferSize="
						+ buffer.getBufferSize());
			}

			if (leftSize > 0) {
				final int rightSize = payloadSize - leftSize;
				_readBuffer.get(buffer.getBytes(), 0, leftSize);
				buffer.clearBytes(leftSize, buffer.getBufferSize() - leftSize
						- rightSize);
				_readBuffer.get(buffer.getBytes(), buffer.getBufferSize()
						- rightSize, rightSize);
			} else {
				_readBuffer.get(buffer.getBytes());
			}

		} catch (IOException ioe) {
			throw new PersistitIOException(ioe);
		}
		return true;
	}

	public synchronized void writePageToLog(final Buffer buffer)
			throws PersistitIOException {

		final Volume volume = buffer.getVolume();
		final int available = buffer.getAvailableSize();
		final int recordSize = LogRecord.PA.OVERHEAD + buffer.getBufferSize()
				- available;
		long address = -1;
		int handle = handleForVolume(volume);

		if (writeBuffer(recordSize)) {
			handle = handleForVolume(volume);
		}
		address = _writeBufferAddress + _writeBuffer.position();
		final int leftSize = available == 0 ? 0 : buffer.getAlloc() - available;
		LogRecord.PA.putLength(_bytes, recordSize);
		LogRecord.PA.putVolumeHandle(_bytes, handle);
		LogRecord.PA.putType(_bytes);
		LogRecord.PA.putTimestamp(_bytes, 0);
		LogRecord.PA.putLeftSize(_bytes, leftSize);
		LogRecord.PA.putPageAddress(_bytes, buffer.getPageAddress());
		_writeBuffer.put(_bytes, 0, LogRecord.PA.OVERHEAD);
		final int payloadSize = recordSize - LogRecord.PA.OVERHEAD;

		if (leftSize > 0) {
			final int rightSize = payloadSize - leftSize;
			_writeBuffer.put(buffer.getBytes(), 0, leftSize);
			_writeBuffer.put(buffer.getBytes(), buffer.getBufferSize()
					- rightSize, rightSize);
		} else {
			_writeBuffer.put(buffer.getBytes());
		}
		final VolumePage vp = new VolumePage(new VolumeDescriptor(volume),
				buffer.getPageAddress());
		final FileAddress fa = new FileAddress(_currentFile, address);
		_pageMap.put(vp, fa);
	}

	public synchronized int handleForVolume(final Volume volume)
			throws PersistitIOException {
		final VolumeDescriptor vd = new VolumeDescriptor(volume);
		Integer handle = _volumeToHandleMap.get(vd);
		if (handle == null) {
			handle = Integer.valueOf(++_handleCounter);
			LogRecord.IV.putType(_bytes);
			LogRecord.IV.putHandle(_bytes, handle.intValue());
			LogRecord.IV.putVolumeId(_bytes, volume.getId());
			LogRecord.IV.putTimestamp(_bytes, 0); // TODO
			LogRecord.IV.putVolumeName(_bytes, volume.getPathName());
			final int recordSize = LogRecord.IV.getLength(_bytes);
			writeBuffer(recordSize);
			_writeBuffer.put(_bytes, 0, recordSize);
			if (_volumeToHandleMap.size() >= MAXIMUM_MAPPED_HANDLES) {
				_volumeToHandleMap.clear();
				_handleToVolumeMap.clear();
			}
			_volumeToHandleMap.put(vd, handle);
			_handleToVolumeMap.put(handle, vd);
		}
		return handle.intValue();
	}

	public synchronized int handleForTree(final Tree tree)
			throws PersistitIOException {
		Integer handle = _treeToHandleMap.get(tree);
		if (handle == null) {
			handle = Integer.valueOf(++_handleCounter);
			LogRecord.IT.putType(_bytes);
			LogRecord.IT.putHandle(_bytes, handle.intValue());
			LogRecord.IT.putTimestamp(_bytes, 0);
			LogRecord.IV.putTimestamp(_bytes, 0); // TODO
			LogRecord.IV.putVolumeName(_bytes, tree.getName());
			final int recordSize = LogRecord.IV.getLength(_bytes);
			writeBuffer(recordSize);
			_writeBuffer.put(_bytes, 0, recordSize);
			if (_treeToHandleMap.size() >= MAXIMUM_MAPPED_HANDLES) {
				_treeToHandleMap.clear();
				_handleToTreeMap.clear();
			}
			_treeToHandleMap.put(tree, handle);
			_handleToTreeMap.put(handle, tree);
		}
		return handle.intValue();
	}

	public synchronized void rollover() throws PersistitIOException {
		_generation++;
		final File file = new File(String.format(PATH_FORMAT, _directory,
				_generation));
		_currentChannel = getFileChannel(file);
		_currentFile = file;
		_fileChannelMap.put(_currentFile, _currentChannel);
		_writeBufferAddress = 0;
		// New copies in every log file
		_handleToTreeMap.clear();
		_handleToVolumeMap.clear();
		_volumeToHandleMap.clear();
		_treeToHandleMap.clear();
	}

	public synchronized void recover() throws PersistitException {
		_pageMap.clear();
		_handleToTreeMap.clear();
		_treeToHandleMap.clear();
		_handleToVolumeMap.clear();
		_volumeToHandleMap.clear();

		final File[] files = files();
		try {
			for (final File file : files) {
				final FileChannel channel = new FileInputStream(file)
						.getChannel();
				long bufferAddress = 0;
				while (bufferAddress < channel.size()) {
					final long size = Math.min(channel.size() - bufferAddress,
							_writeBufferSize);
					final MappedByteBuffer readBuffer = channel.map(
							MapMode.READ_ONLY, bufferAddress, size);
					while (recoverOneRecord(file, bufferAddress, readBuffer)) {

					}
					bufferAddress += readBuffer.position();
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace(); // TODO
		} catch (LogNotClosedException e) {
			// ok - this is normal when recovering after a dirty shutdown
		}
		_recovered = true;
	}

	private boolean recoverOneRecord(final File file, final long bufferAddress,
			final MappedByteBuffer readBuffer) throws PersistitException,
			LogNotClosedException {
		final int from = readBuffer.position();
		if (readBuffer.remaining() < LogRecord.OVERHEAD) {
			return false;
		}
		readBuffer.mark();
		readBuffer.get(_bytes, 0, LogRecord.OVERHEAD);
		final char type = LogRecord.getType(_bytes);
		switch (type) {
		
		case LogRecord.TYPE_IV: {
			final int recordSize = LogRecord.getLength(_bytes);
			if (recordSize > LogRecord.IV.MAX_LENGTH) {
				throw new CorruptLogException("IV LogRecord too long: "
						+ recordSize
						+ " bytes at position "
						+ new FileAddress(file, bufferAddress
								+ readBuffer.position() - LogRecord.OVERHEAD));
			}
			if (recordSize + from > readBuffer.limit()) {
				readBuffer.reset();
				return false;
			}
			readBuffer.get(_bytes, LogRecord.OVERHEAD, recordSize
					- LogRecord.OVERHEAD);
			final Integer handle = Integer.valueOf(LogRecord.IV
					.getHandle(_bytes));
			final String path = LogRecord.IV.getVolumeName(_bytes);
			final long volumeId = LogRecord.IV.getVolumeId(_bytes);
			VolumeDescriptor vd = new VolumeDescriptor(path, volumeId);
			_handleToVolumeMap.put(handle, vd);
			_volumeToHandleMap.put(vd, handle);
			break;
		}
		
		case LogRecord.TYPE_IT: {
			final int recordSize = LogRecord.getLength(_bytes);
			if (recordSize > LogRecord.IT.MAX_LENGTH) {
				throw new CorruptLogException("IT LogRecord too long: "
						+ recordSize
						+ " bytes at position "
						+ new FileAddress(file, bufferAddress
								+ readBuffer.position() - LogRecord.OVERHEAD));
			}
			if (recordSize + from > readBuffer.limit()) {
				readBuffer.reset();
				return false;
			}
			readBuffer.get(_bytes, LogRecord.OVERHEAD, recordSize
					- LogRecord.OVERHEAD);
			final Integer handle = Integer.valueOf(LogRecord.IT
					.getHandle(_bytes));
			final String path = LogRecord.IT.getTreeName(_bytes);
			final Integer volumeHandle = Integer.valueOf(LogRecord.IT
					.getVolumeHandle(_bytes));
			final Integer treeHandle = Integer.valueOf(LogRecord.IT
					.getHandle(_bytes));
			// TODO
			break;
		}
		
		case LogRecord.TYPE_PA: {
			final int recordSize = LogRecord.getLength(_bytes);
			if (recordSize > Buffer.MAX_BUFFER_SIZE + LogRecord.PA.OVERHEAD) {
				throw new CorruptLogException("PA LogRecord too long: "
						+ recordSize
						+ " bytes at position "
						+ new FileAddress(file, bufferAddress
								+ readBuffer.position() - LogRecord.OVERHEAD));
			}
			if (recordSize + from > readBuffer.limit()) {
				readBuffer.reset();
				return false;
			}
			readBuffer.get(_bytes, LogRecord.OVERHEAD, LogRecord.PA.OVERHEAD
					- LogRecord.OVERHEAD);
			readBuffer.position(from + recordSize);
			final long address = bufferAddress + from;
			final long pageAddress = LogRecord.PA.getPageAddress(_bytes);
			final Integer volumeHandle = Integer.valueOf(LogRecord.PA
					.getVolumeHandle(_bytes));
			final VolumeDescriptor vd = _handleToVolumeMap.get(volumeHandle);
			if (vd == null) {
				throw new CorruptLogException(
						"PA reference to volume "
								+ volumeHandle
								+ " is not preceded by an IV record for that handle at "
								+ new FileAddress(file, address));
			}
			final VolumePage vp = new VolumePage(vd, pageAddress);
			final FileAddress fa = new FileAddress(file, address);
			_pageMap.put(vp, fa);
			break;
		}

		case LogRecord.TYPE_RR:
		case LogRecord.TYPE_WR:
		case LogRecord.TYPE_TS:
		case LogRecord.TYPE_TC:
		case LogRecord.TYPE_TJ:
			throw new UnsupportedOperationException(
					"Can't handle record of type " + (int) type);

		default:
			_persistit.getLogBase().log(LogBase.LOG_INIT_RECOVER_TERMINTATE,
					new FileAddress(file, bufferAddress + from));
			throw new LogNotClosedException();
		}
		return true;
	}

	public synchronized void close() throws PersistitIOException {
		try {
			closeChannel();
		} catch (IOException ioe) {
			throw new PersistitIOException(ioe);
		}
		if (_flushTimer != null) {
			_flushTimer.cancel();
			_flushTimer = null;
		}
		_fileChannelMap.clear();
		_handleToTreeMap.clear();
		_handleToVolumeMap.clear();
		_volumeToHandleMap.clear();
		_treeToHandleMap.clear();
		_pageMap.clear();
		_currentFile = null;
		_currentChannel = null;
		_writeBuffer = null;
		_readBuffer = null;
		_persistit = null;
		Arrays.fill(_bytes, (byte) 0);
	}

	public void force() throws PersistitIOException {
		final MappedByteBuffer mbb;
		synchronized (this) {
			mbb = _writeBuffer;
		}

		if (mbb != null) {
			mbb.force();
		}
	}

	private boolean writeBuffer(final int size) throws PersistitIOException {
		boolean rolled = false;
		if (_writeBuffer != null) {
			if (_writeBuffer.remaining() >= size) {
				return false;
			} else {
				_writeBufferAddress += _writeBuffer.position();
				_writeBuffer.force();
				_writeBuffer = null;
			}
		}

		if (_currentChannel == null
				|| _writeBufferAddress + _writeBufferSize > _maximumFileSize) {
			rollover();
			rolled = true;
		}

		try {
			_writeBuffer = _currentChannel.map(MapMode.READ_WRITE,
					_writeBufferAddress, _writeBufferSize);
		} catch (IOException ioe) {
			throw new PersistitIOException(ioe);
		}
		return rolled;
	}

	private void closeChannel() throws IOException {
		if (_writeBuffer != null) {
			_writeBuffer.force();
			_writeBufferAddress += _writeBuffer.position();
			_writeBuffer = null;
		}
		if (_currentChannel != null) {
			_currentChannel.truncate(_writeBufferAddress);
			_currentChannel.force(true);
			_currentChannel.close();
		}
		_writeBufferAddress = 0;
	}

	private File[] files() {
		File file = _directory;
		if (!file.isDirectory()) {
			file = file.getParentFile();
			if (file == null) {
				file = new File(".");
			}
		}
		final File dir = file;

		final File[] files = dir.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				return pathname.getParent().startsWith(dir.getPath())
						&& Pattern.matches(PATH_PATTERN, pathname.getPath());
			}

		});
		if (files == null) {
			return new File[0];
		}
		Arrays.sort(files);
		return files;
	}

	private synchronized FileChannel getFileChannel(final File file)
			throws PersistitIOException {
		try {
			FileChannel fc = _fileChannelMap.get(file);
			if (fc == null) {
				final RandomAccessFile raf = new RandomAccessFile(file, "rw");
				fc = raf.getChannel();
				_fileChannelMap.put(file, fc);
			}
			return fc;
		} catch (IOException ioe) {
			throw new PersistitIOException(ioe);
		}
	}

	private static class VolumeDescriptor {

		private final long id;

		private final String path;

		VolumeDescriptor(final String path, final long id) {
			this.path = path;
			this.id = id;
		}

		VolumeDescriptor(final Volume volume) {
			this.path = volume.getPathName();
			this.id = volume.getId();
		}

		String getPath() {
			return path;
		}

		long getId() {
			return id;
		}

		@Override
		public boolean equals(final Object object) {
			final VolumeDescriptor vd = (VolumeDescriptor) object;
			return vd.path.equals(path) && vd.id == id;
		}

		@Override
		public int hashCode() {
			return path.hashCode() ^ (int) id;
		}

		@Override
		public String toString() {
			return path;
		}

	}

	private static class VolumePage {

		private final VolumeDescriptor volumeDescriptor;

		private final long page;

		VolumePage(final VolumeDescriptor vd, final long page) {
			this.volumeDescriptor = vd;
			this.page = page;
		}

		VolumeDescriptor getVolumeDescriptor() {
			return volumeDescriptor;
		}

		long getPage() {
			return page;
		}

		@Override
		public int hashCode() {
			return volumeDescriptor.hashCode() ^ (int) page
					^ (int) (page >>> 32);
		}

		@Override
		public boolean equals(Object obj) {
			final VolumePage vp = (VolumePage) obj;
			return page == vp.page && volumeDescriptor.equals(volumeDescriptor);
		}

		@Override
		public String toString() {
			return volumeDescriptor + ":" + page;
		}
	}

	private static class FileAddress {

		private final File file;

		private final long address;

		FileAddress(final File file, final long address) {
			this.file = file;
			this.address = address;
		}

		File getFile() {
			return file;
		}

		long getAddress() {
			return address;
		}

		@Override
		public int hashCode() {
			return file.hashCode() ^ (int) address ^ (int) (address >>> 32);
		}

		@Override
		public boolean equals(final Object object) {
			final FileAddress fa = (FileAddress) object;
			return file.equals(fa.file) && address == fa.address;
		}

		@Override
		public String toString() {
			return file + ":" + address;
		}
	}

}
