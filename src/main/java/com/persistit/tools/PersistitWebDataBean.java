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
 * 
 * Created on Apr 19, 2005
 */
package com.persistit.tools;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.persistit.DefaultObjectCoder;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Persistit;
import com.persistit.exception.PersistitException;

/**
 * @author Peter Beaman
 * @version 1.0
 */
public class PersistitWebDataBean {
	private static PrintStream PRINT_STREAM;

	private final static String DATA_PATH = "../data";

	private final static Persistit PERSISTIT_INSTANCE = new Persistit();

	private final static Pattern URL_PATTERN = Pattern
			.compile("((\\w+\\-+)|(\\w+\\.))*\\w{1,63}\\.[a-zA-Z]{2,6}$");

	private final static Pattern EMAIL_PATTERN = Pattern
			.compile("^(([A-Za-z0-9]+_+)|([A-Za-z0-9]+\\-+)|([A-Za-z0-9]+\\.+)|([A-Z"
					+ "a-z0-9]+\\++))*[A-Za-z0-9]+@((\\w+\\-+)|(\\w+\\.))*\\w{1,63}\\.[a-z"
					+ "A-Z]{2,6}$");

	public final static SimpleDateFormat SDF = new SimpleDateFormat(
			"yyyyMMddHHmmssZ");

	private final static Map CONTACT_RECORD_FIELD_MAP = new HashMap();

	private final static Field[] CONTACT_RECORD_FIELDS = {
			new Field(0, "ContactId", "long", 1, 12, null, ""),
			new Field(1, "FName", "String", 1, 99, null, "contact,download"),
			new Field(2, "LName", "String", 2, 99, null, "contact,download"),
			new Field(3, "Company", "String", 1, 256, null, "contact,download"),
			new Field(4, "CompanyUrl", "String", 1, 256, URL_PATTERN, ""),
			new Field(5, "EMailAddr", "String", 1, 256, EMAIL_PATTERN,
					"contact,download"),
			new Field(6, "Phone", "String", 1, 256, null, "download"),
			new Field(7, "Comments", "String", 1, 65536, null, ""),
			new Field(8, "MailingList", "Boolean", 1, 10, null, ""),
			new Field(9, "AcceptLicense", "Boolean", 1, 10, null, "download"), };

	private static class Field {
		int _index;

		String _name;

		String _type;

		int _minLength;

		int _maxLength;

		Pattern _pattern;

		String _mandatoryFlags;

		Field(int index, String name, String type, int minLength,
				int maxLength, Pattern pattern, String mandatoryFlags) {
			_index = index;
			_name = name;
			_type = type;
			_minLength = minLength;
			_maxLength = maxLength;
			_pattern = pattern;
			_mandatoryFlags = mandatoryFlags;
			CONTACT_RECORD_FIELD_MAP.put(name, this);
		}
	}

	public static class ConfigRecord {
		static {
			DefaultObjectCoder.registerObjectCoder(PERSISTIT_INSTANCE,
					ConfigRecord.class, new String[] { "_type", },
					new String[] { "_downloadFileName", "_downloadMimeType",
							"_suggestedFileName", "_versionTimestamp",
							"_downloadMessageTemplate", "_emailHostAddr",
							"_emailReplyAddr", "_emailSubject",
							"_persistitVersionName", });
		}

		String _type = "eval";

		String _downloadFileName = DATA_PATH
				+ "/persistit_jsa110_140.20050425.zip";

		String _downloadMimeType = "application/x-zip-compressed";

		String _suggestedFileName = "persistit_jsa110_kit.zip";

		String _versionTimestamp = "20050425014000";

		String _downloadMessageTemplate = "download_eval_template.html";

		String _emailHostAddr = "mail.kattare.com";

		String _emailReplyAddr = "info@persistit.com";

		String _emailSubject = "Persistit JSA 1.1 Evaluation Kit";

		String _persistitVersionName = "Persistit JSA 1.1";

		public String getDownloadFileName() {
			return _downloadFileName;
		}

		public String getDownloadMimeType() {
			return _downloadMimeType;
		}

		public String getSuggestedFileName() {
			return _suggestedFileName;
		}

		public String getVersionTimestamp() {
			return _versionTimestamp;
		}

		public String getDownloadTemplate() {
			return _downloadMessageTemplate;
		}

		public String getEmailHost() {
			return _emailHostAddr;
		}

		public String getEmailReplyAddr() {
			return _emailReplyAddr;
		}

		public String getEmailSubject() {
			return _emailSubject;
		}

		public String getPersistitVersionName() {
			return _persistitVersionName;
		}

		public void loadFromRequest(HttpServletRequest request) {
			_downloadFileName = getParam(request, "downloadFileName",
					_downloadFileName);
			_downloadMimeType = getParam(request, "downloadMimeType",
					_downloadMimeType);
			_suggestedFileName = getParam(request, "suggestedFileName",
					_suggestedFileName);
			_versionTimestamp = getParam(request, "versionTS",
					_versionTimestamp);
			_downloadMessageTemplate = getParam(request,
					"downloadMessageTemplate", _downloadMessageTemplate);
			_emailHostAddr = getParam(request, "emailHostAddr", _emailHostAddr);
			_emailReplyAddr = getParam(request, "emailReplyAddr",
					_emailReplyAddr);
			_emailSubject = getParam(request, "emailSubject", _emailSubject);
			_persistitVersionName = getParam(request, "persistitVersionName",
					_persistitVersionName);
		}
	}

	public static class ContactRecord {
		String _recordType;

		long _contactId;

		long _registeredTime;

		String _remoteHostName;

		String _loginName;

		String _passwordHash;

		String _elaVersion;

		String _evalStartDate;

		String _evalEndDate;

		String _downloadKey;

		long _updateTime;

		String[] _fieldValues = new String[CONTACT_RECORD_FIELDS.length];

		static {
			DefaultObjectCoder.registerObjectCoder(PERSISTIT_INSTANCE,
					ContactRecord.class, new String[] { "_contactId", },
					new String[] { "_recordType", "_registeredTime",
							"_remoteHostName", "_loginName", "_passwordHash",
							"_elaVersion", "_evalStartDate", "_evalEndDate",
							"_downloadKey", "_updateTime", "_fieldValues", });
		}

		public long getContactId() {
			return _contactId;
		}

		public String getEvalStartDate() {
			return _evalStartDate;
		}

		public String getEvalEndDate() {
			return _evalEndDate;
		}

		public String getDownloadKey() {
			return _downloadKey;
		}

		public String getUpdateTimestamp() {
			if (_updateTime <= 0)
				return "none";
			else
				return SDF.format(new Date(_updateTime));
		}

		private Field lookupField(String name) {
			Field field = (Field) CONTACT_RECORD_FIELD_MAP.get(name);
			if (field == null) {
				throw new RuntimeException("No such field named " + name);
			}
			return field;
		}

		private void putField(String name, String value) {
			Field field = lookupField(name);
			_fieldValues[field._index] = value;
		}

		private void loadFromRequest(HttpServletRequest request) {
			for (int index = 0; index < CONTACT_RECORD_FIELDS.length; index++) {
				Field field = CONTACT_RECORD_FIELDS[index];
				String value = request.getParameter(field._name);
				if (value != null && value.length() > field._maxLength + 10) {
					value = value.substring(0, field._maxLength + 10);
				}
				_fieldValues[index] = value;
			}
			_remoteHostName = getRemoteAddr(request);
		}

		public String getFormValue(String name) {
			Field field = lookupField(name);
			if (field == null)
				return null;
			String value = _fieldValues[field._index];
			return value == null ? "" : value;
		}

		public String getFormErrorMessage(String name) {
			Field field = lookupField(name);
			if (field == null)
				return null;
			String value = _fieldValues[field._index];
			return errorMessage(field, value);
		}

		public String getLicensee() {
			return getFormValue("FName") + " " + getFormValue("LName") + " / "
					+ getFormValue("Company");
		}

		public String getCustom1() {
			StringBuffer sb = new StringBuffer();
			sb.append(getFormValue("EMailAddr"));
			sb.append(" ");
			sb.append(getFormValue("Phone"));
			return sb.toString().trim();
		}

		public String getCustom2() {
			StringBuffer sb = new StringBuffer();
			sb.append(getFormValue("CompanyUrl"));
			sb.append(" ");
			sb.append(getFormValue("Company"));
			return sb.toString().trim();
		}

		public String getRemoteHostName() {
			return _remoteHostName;
		}

		private String errorMessage(Field field, String value) {
			if (value == null)
				value = "";
			if (value.length() == 0) {
				if (_recordType != null
						&& field._mandatoryFlags.indexOf(_recordType) == -1) {
					return null;
				}
				if (field._minLength == 0 && field._pattern == null) {
					return null;
				} else
					return "Required";
			}
			if (value.length() < field._minLength) {
				return "Minimum length is " + field._minLength;
			}
			if (value.length() > field._maxLength) {
				return "Maximum length is " + field._maxLength;
			} else if (field._pattern != null) {
				Matcher matcher = field._pattern.matcher(value);
				if (!matcher.matches()) {
					return "Invalid format";
				}
			}
			return null;
		}

		public boolean isValid() {
			for (int index = 0; index < CONTACT_RECORD_FIELDS.length; index++) {
				String s = errorMessage(CONTACT_RECORD_FIELDS[index],
						_fieldValues[index]);
				if (s != null)
					return false;
			}
			return true;
		}

		public void setupDownloadKey() {
			_evalStartDate = MakeLicense.licenseDate(0);
			_evalEndDate = MakeLicense.licenseDate(61);
			int hash1 = (_evalStartDate.hashCode() ^ _evalEndDate.hashCode()
					^ getFormValue("FName").hashCode()
					^ getFormValue("LName").hashCode() ^ getFormValue(
					"EMailAddr").hashCode()) & 0xFFFFFF;
			long minutes = System.currentTimeMillis() / 60000;
			int hash2 = (int) (minutes & 0xFFFFFF);
			_downloadKey = "JSA_" + hash1 + "_" + hash2;
		}

		public String toString() {
			StringBuffer sb = new StringBuffer("ContactRecord(");
			sb.append("id=");
			sb.append(_contactId);
			sb.append(",remoteHost=");
			sb.append(_remoteHostName);
			sb.append(",evalStart=");
			sb.append(_evalStartDate);
			sb.append(",evalEnd=");
			sb.append(_evalEndDate);
			sb.append(",dkey=");
			sb.append(_downloadKey);
			for (int index = 0; index < CONTACT_RECORD_FIELDS.length; index++) {
				Field field = CONTACT_RECORD_FIELDS[index];
				sb.append(",");
				sb.append(field._name);
				sb.append("=");
				sb.append(_fieldValues[index]);
			}
			sb.append(",isValid=");
			sb.append(isValid());
			sb.append(")");
			return sb.toString();
		}
	}

	public static class DownloadRecord {
		private long _startTime;

		private long _finishTime;

		private String _error;

		private String _downloadKey;

		private String _remoteHostName;

		static {
			DefaultObjectCoder.registerObjectCoder(PERSISTIT_INSTANCE,
					DownloadRecord.class, new String[] { "_startTime",
							"_downloadKey" }, new String[] { "_finishTime",
							"_error", "_remoteHostName" });
		}

		public void start(String downloadKey, String remoteHostName) {
			_error = null;
			_downloadKey = downloadKey;
			_remoteHostName = remoteHostName;
			_startTime = System.currentTimeMillis();
			_finishTime = 0;
		}

		public void finish(String error) {
			_error = error;
			_finishTime = System.currentTimeMillis();
		}

		public long getStartTime() {
			return _startTime;
		}

		public String getDisplayableStartTime() {
			if (_startTime <= 0)
				return "none";
			else
				return SDF.format(new Date(_startTime));
		}

		public long getFinishTime() {
			return _finishTime;
		}

		public String getDisplayableFinishTime() {
			if (_finishTime <= 0)
				return "none";
			else
				return SDF.format(new Date(_finishTime));
		}

		public String getDownloadKey() {
			return _downloadKey;
		}

		public String getError() {
			return _error;
		}

		public String getRemoteHostName() {
			return _remoteHostName;
		}

		/**
		 * private long _startTime; private long _finishTime; private String
		 * _error; private String _downloadKey; private String _remoteHostName;
		 */
		public String toString() {
			StringBuffer sb = new StringBuffer("DownloadRecord(");
			sb.append("startTime=");
			sb.append(_startTime);
			sb.append(",finishTime=");
			sb.append(_finishTime);
			sb.append(",dkey=");
			sb.append(_downloadKey);
			sb.append(",remoteHost=");
			sb.append(_remoteHostName);
			sb.append(")");
			return sb.toString();
		}
	}

	public String getFormValue(HttpServletRequest request, String name) {
		ContactRecord record = (ContactRecord) request.getSession()
				.getAttribute("ContactRecord");
		if (record != null)
			return record.getFormValue(name);
		return "";
	}

	public boolean getBooleanFormValue(HttpServletRequest request, String name) {
		String value = getFormValue(request, name);
		return ("on".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value) || "yes"
				.equalsIgnoreCase(value));
	}

	public String getFormErrorMessage(HttpServletRequest request, String name) {
		ContactRecord record = (ContactRecord) request.getSession()
				.getAttribute("ContactRecord");
		if (record != null)
			return record.getFormErrorMessage(name);
		return null;
	}

	public static ContactRecord getContactRecord(HttpServletRequest request)
			throws PersistitException {
		long contactId = -1;
		try {
			contactId = Long.parseLong(request.getParameter("contactId"));
		} catch (Exception e) {
			return null;
		}
		initializePersistit(realPath(DATA_PATH, request));
		ContactRecord record = new ContactRecord();
		Exchange exchange = getContactExchange();

		exchange.clear().append(contactId).fetch();
		if (exchange.getValue().isDefined()) {
			exchange.getValue().get(record);
			record._contactId = contactId;
			return record;
		} else {
			return null;
		}
	}

	public static ConfigRecord getConfigRecord(HttpServletRequest request)
			throws PersistitException {
		initializePersistit(realPath(DATA_PATH, request));
		ConfigRecord record = new ConfigRecord();
		Exchange exchange = getConfigExchange();
		String configKey = request.getParameter("configKey");
		if (configKey == null)
			configKey = "eval";
		exchange.clear().append(configKey).fetch();
		if (exchange.getValue().isDefined()) {
			exchange.getValue().get(record);
		}
		return record;
	}

	public ConfigRecord postConfigRecord(HttpServletRequest request)
			throws IOException, PersistitException {
		initializePersistit(realPath(DATA_PATH, request));
		ConfigRecord record = new ConfigRecord();
		record.loadFromRequest(request);
		Exchange exchange = getConfigExchange();
		exchange.getValue().put(record);
		String configKey = request.getParameter("configKey");
		if (configKey == null)
			configKey = "eval";
		exchange.clear().append(configKey).store();
		PERSISTIT_INSTANCE.flush(); // never know when that Tomcat instance will
		// go down
		return record;
	}

	public ContactRecord postContactInfo(HttpServletRequest request)
			throws IOException, PersistitException {
		initializePersistit(realPath(DATA_PATH, request));
		ContactRecord record = (ContactRecord) request.getSession()
				.getAttribute("ContactRecord");
		if (record == null) {
			record = new ContactRecord();
			request.getSession().setAttribute("ContactRecord", record);
		}
		record._recordType = "contact";
		record.loadFromRequest(request);
		if (record.isValid()) {
			assignUniqueId(record);
			Exchange ex = getContactExchange();
			ex.getValue().put(record);
			ex.clear().append(record._contactId).store();
			PERSISTIT_INSTANCE.releaseExchange(ex);
			PERSISTIT_INSTANCE.flush(); // never know when that Tomcat instance
			// will go down
		}
		return record;
	}

	public ContactRecord postDownloadInfo(HttpServletRequest request)
			throws IOException, PersistitException {
		initializePersistit(realPath(DATA_PATH, request));
		ConfigRecord config = getConfigRecord(request);
		ContactRecord record = (ContactRecord) request.getSession()
				.getAttribute("ContactRecord");
		if (record == null) {
			record = new ContactRecord();
			request.getSession().setAttribute("ContactRecord", record);
		}
		record._recordType = "download";
		record.loadFromRequest(request);

		if (record.isValid()) {
			assignUniqueId(record);
			record.setupDownloadKey();
			Exchange ex = getContactExchange();
			ex.getValue().put(record);
			ex.clear().append(record._contactId).store();
			ex.getValue().putNull();
			ex.clear().append("byDownloadKey").append(record._downloadKey)
					.append(record._contactId).store();
			PERSISTIT_INSTANCE.releaseExchange(ex);
			PERSISTIT_INSTANCE.flush(); // never know when that Tomcat instance
			// will go down
			mailToContact(record,
					new StringBuffer(config.getDownloadTemplate()), config);
		}

		return record;
	}

	public ContactRecord nextContactRecord(HttpServletRequest request,
			ContactRecord record) {
		try {
			initializePersistit(realPath(DATA_PATH, request));
			if (record == null) {
				record = new ContactRecord();
				try {
					record._contactId = Long.parseLong(request
							.getParameter("fromId"));
				} catch (Exception e) {
					// ignore if missing or invalid fromId parameter.
				}
			}
			Exchange exchange = getContactExchange();
			exchange.clear().append(record._contactId);
			if (!exchange.next())
				return null;
			if (exchange.getKey().indexTo(0).decodeType() != Long.class)
				return null;
			record._contactId = exchange.getKey().decodeLong();
			exchange.getValue().get(record);
			return record;
		} catch (PersistitException pe) {
			pe.printStackTrace();
			return null;
		}

	}

	public DownloadRecord nextDownloadRecord(HttpServletRequest request,
			DownloadRecord record) {
		try {
			initializePersistit(realPath(DATA_PATH, request));
			if (record == null)
				record = new DownloadRecord();
			Exchange exchange = getDownloadExchange();
			exchange.clear().append(record);
			if (!exchange.next())
				return null;
			exchange.getKey().indexTo(0).decode(record);
			exchange.getValue().get(record);
			return record;
		} catch (PersistitException pe) {
			pe.printStackTrace();
			return null;
		}
	}

	private void assignUniqueId(ContactRecord record) throws PersistitException {
		// if (record._contactId != 0) return;
		Exchange ex = getContactExchange();
		long newId = ex.clear().append("idCounter").incrementValue();
		record._contactId = newId;
		record._updateTime = System.currentTimeMillis();
		PERSISTIT_INSTANCE.releaseExchange(ex);
	}

	private static String getRemoteAddr(HttpServletRequest request) {
		String remoteAddr = request.getHeader("X-Forwarded-For");
		if (remoteAddr == null || remoteAddr.length() == 0) {
			remoteAddr = request.getRemoteAddr();
		}
		return remoteAddr;
	}

	public static void doGetForDownload(HttpServletRequest request,
			HttpServletResponse response) throws IOException {
		String downloadKey = request.getParameter("id");
		ContactRecord contactRecord = null;
		DownloadRecord downloadRecord = null;
		ConfigRecord config = null;
		String error = "Missing or invalid download key";
		try {
			initializePersistit(realPath(DATA_PATH, request));
			config = getConfigRecord(request);
			downloadRecord = new DownloadRecord();
			downloadRecord.start(downloadKey, getRemoteAddr(request));
			Exchange ex = getContactExchange();
			ex.clear().append("byDownloadKey").append(downloadKey).append(
					Key.AFTER);
			if (ex.previous()) {
				long contactId = ex.getKey().indexTo(2).decodeLong();
				ex.clear().append(contactId).fetch();
				if (ex.getValue().isDefined()) {
					contactRecord = (ContactRecord) ex.getValue().get();
				}
			}
			PERSISTIT_INSTANCE.releaseExchange(ex);
		} catch (PersistitException pe) {
			error = "Exception: " + pe.toString();
		}

		if (contactRecord != null && downloadKey != null
				&& downloadKey.equals(contactRecord.getDownloadKey())) {
			Properties props = new Properties();

			props.setProperty(MakeLicense.PROP_VERSION, config
					.getPersistitVersionName());
			props.setProperty(MakeLicense.PROP_LICENSEE, contactRecord
					.getLicensee());
			props.setProperty(MakeLicense.PROP_CUSTOM1, contactRecord
					.getCustom1());
			props.setProperty(MakeLicense.PROP_CUSTOM2, contactRecord
					.getCustom2());
			props.setProperty(MakeLicense.PROP_START, contactRecord
					.getEvalStartDate());
			props.setProperty(MakeLicense.PROP_END, contactRecord
					.getEvalEndDate());

			InputStream is = new FileInputStream(realPath(config
					.getDownloadFileName(), request));
			OutputStream os = response.getOutputStream();
			response.setContentType(config.getDownloadMimeType());
			response.setHeader("Content-Disposition", "attachment;filename="
					+ config.getSuggestedFileName());

			try {
				MakeLicense.rewriteKit(is, os, props, config
						.getVersionTimestamp());
				error = null;
			} catch (Exception e) {
				error = "Exception: " + e.toString();
			}
		}
		downloadRecord.finish(error);
		try {
			Exchange ex = PersistitWebDataBean.getDownloadExchange();
			ex.getValue().put(downloadRecord);
			ex.clear().append(downloadRecord).store();
			PERSISTIT_INSTANCE.releaseExchange(ex);
			PERSISTIT_INSTANCE.flush();
		} catch (PersistitException pe) {
			// just ignore this one.
		}
		if (error != null) {
			response.sendRedirect("../download_error.jsp?reason=" + error);
		}
	}

	private static String realPath(String path, HttpServletRequest request) {
		ServletContext context = request.getSession().getServletContext();
		return context.getRealPath(path);
	}

	private static String getParam(HttpServletRequest request,
			String parameterName, String oldValue) {
		String value = request.getParameter(parameterName);
		if (value == null)
			value = oldValue;
		return value;
	}

	private static void initializePersistit(String path)
			throws PersistitException {
		if (PERSISTIT_INSTANCE.isInitialized())
			return;
		try {
			String outFile = path + "/system_out.txt";
			PRINT_STREAM = new PrintStream(new FileOutputStream(outFile, true));
			PRINT_STREAM.println();
			PRINT_STREAM
					.println("============================================");
			PRINT_STREAM.println("Starting PersistitWebDataBean at "
					+ new Date());
			PRINT_STREAM
					.println("============================================");
			PRINT_STREAM.println();

			Properties props = new Properties();

			props.setProperty("datapath", path);
			props.setProperty("logpath", path);

			props.setProperty("logfile", "${logpath}/persistit.log");

			props.setProperty("buffer.count.8192", "256");

			props
					.setProperty(
							"volume.1",
							"${datapath}/Persistit_system_txn.v01,create,pageSize:8K,"
									+ "initialSize:20K,extensionSize:20K,maximumSize:10G");

			props
					.setProperty(
							"volume.2",
							"${datapath}/ContactData.v01,create,pageSize:8K,"
									+ "initialSize:1M,extensionSize:1M,maximumSize:10G");

			props
					.setProperty(
							"volume.3",
							"${datapath}/ConfigData.v01,create,pageSize:8K,"
									+ "initialSize:20K,extensionSize:20K,maximumSize:1G");

			props.setProperty("pwjpath", "${datapath}/PersistitWebData.pwj");

			props.setProperty("pwjsize", "512K");

			props.setProperty("pwjdelete", "true");

			props.setProperty("pwjcount", "1");

			PERSISTIT_INSTANCE.initialize(props);
		} catch (IOException ioe) {
			PRINT_STREAM
					.println("Exception while initializing Persist: " + ioe);
			ioe.printStackTrace(PRINT_STREAM);
		} catch (PersistitException pe) {
			PRINT_STREAM.println("Exception while initializing Persist: " + pe);
			pe.printStackTrace(PRINT_STREAM);
			throw pe;
		}
	}
	
	public static void close() throws PersistitException {
		PERSISTIT_INSTANCE.close();
	}

	private static Exchange getContactExchange() throws PersistitException {
		return PERSISTIT_INSTANCE.getExchange("ContactData", "contacts", true);
	}

	private static Exchange getConfigExchange() throws PersistitException {
		return PERSISTIT_INSTANCE.getExchange("ConfigData", "config", true);
	}

	private static Exchange getDownloadExchange() throws PersistitException {
		return PERSISTIT_INSTANCE.getExchange("ContactData", "downloads", true);
	}

	private void mailToContact(ContactRecord record, StringBuffer sb,
			ConfigRecord config) {

		for (int index = 0; index < CONTACT_RECORD_FIELDS.length; index++) {
			Field field = CONTACT_RECORD_FIELDS[index];
			substitute(sb, field._name, record.getFormValue(field._name));
		}
		substitute(sb, "start", record._evalStartDate);
		substitute(sb, "end", record._evalEndDate);
		substitute(sb, "downloadKey", record._downloadKey);
		substitute(sb, "licensee", record.getLicensee());

		String message = sb.toString();
		String to = record.getFormValue("EMailAddr");

		Properties props = new Properties();
		props.put("mail.smtp.host", config.getEmailHost());
		props.put("mail.debug", "false");
		Session xsession = Session.getInstance(props);

		try {
			Message msg = new MimeMessage(xsession);
			msg.setFrom(new InternetAddress(config._emailReplyAddr));
			InternetAddress[] address = { new InternetAddress(to) };
			msg.setRecipients(Message.RecipientType.TO, address);
			msg.setSubject(config.getEmailSubject());
			msg.setSentDate(new Date());

			MimeMultipart mmp = new MimeMultipart();
			MimeBodyPart mbp = new MimeBodyPart();
			mbp.setContent(sb.toString(), "text/html");
			mmp.addBodyPart(mbp);
			msg.setContent(mmp);

			Transport.send(msg);
		} catch (MessagingException mex) {
			mex.printStackTrace(PRINT_STREAM);
		}
	}

	static void substitute(StringBuffer sb, String name, String value) {
		String from = "${" + name + "}";
		for (int p = 0; p != -1;) {
			p = sb.indexOf(from, p);
			if (p >= 0) {
				sb.replace(p, p + from.length(), value);
				p += value.length();
			}
		}
	}

	public static void mailToWebmaster(HttpServletRequest request,
			String message) {
		try {
			String to = "webmaster@persistit.com";
			StringBuffer sb = new StringBuffer(message);
			sb.append("\r\n--headers--");

			for (Enumeration headerNames = request.getHeaderNames(); headerNames != null
					&& headerNames.hasMoreElements();) {
				String headerName = (String) headerNames.nextElement();
				sb.append("\r\n");
				sb.append(headerName);
				sb.append(" = ");
				sb.append(request.getHeader(headerName));
			}
			sb.append("\r\n--parameters--");
			for (Enumeration paramNames = request.getParameterNames(); paramNames != null
					&& paramNames.hasMoreElements();) {
				String paramName = (String) paramNames.nextElement();
				sb.append("\r\n");
				sb.append(paramName);
				sb.append(" = ");
				sb.append(request.getParameter(paramName));
			}
			sb.append("\r\n--now--\r\n");
			sb.append(new Date());
			sb.append("\r\n");

			ConfigRecord config = getConfigRecord(request);

			Properties props = new Properties();
			props.put("mail.smtp.host", config.getEmailHost());
			props.put("mail.debug", "false");
			Session xsession = Session.getInstance(props);

			Message msg = new MimeMessage(xsession);
			msg.setFrom(new InternetAddress(config._emailReplyAddr));
			InternetAddress[] address = { new InternetAddress(to) };
			msg.setRecipients(Message.RecipientType.TO, address);
			msg.setSubject("auto");
			msg.setSentDate(new Date());

			MimeMultipart mmp = new MimeMultipart();
			MimeBodyPart mbp = new MimeBodyPart();
			mbp.setContent(sb.toString(), "text/plain");
			mmp.addBodyPart(mbp);
			msg.setContent(mmp);

			Transport.send(msg);
		} catch (MessagingException mex) {
			mex.printStackTrace(PRINT_STREAM);
		} catch (PersistitException pe) {
			pe.printStackTrace(PRINT_STREAM);
		}

	}

	public static boolean login(HttpServletRequest request, String username,
			String password) {
		boolean okay = false;
		if (password != null && username != null)
			okay = "admin".equals(username) && password.length() == 9
					&& password.charAt(0) == '3' && password.charAt(7) == 's'
					&& password.charAt(2) == '1' && password.charAt(5) == 'a'
					&& password.charAt(3) == 'u' && password.charAt(1) == '5'
					&& password.charAt(4) == 'z' && password.charAt(6) == 'c'
					&& password.charAt(8) == 'e';
		if (okay) {
			request.getSession().setAttribute("loginexpires",
					new Long(System.currentTimeMillis() + 900000)); // 15
			// minutes
		} else {
			request.getSession().removeAttribute("login");
		}
		return okay;
	}

	public static boolean isLoggedIn(HttpServletRequest request) {
		Long expires = (Long) request.getSession().getAttribute("loginexpires");
		return expires != null
				&& System.currentTimeMillis() < expires.longValue();
	}
}
