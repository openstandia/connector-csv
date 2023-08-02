package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.Column;
import com.evolveum.polygon.connector.csv.util.Util;
import com.google.gson.Gson;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.spi.SyncTokenResultsHandler;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

import static com.evolveum.polygon.connector.csv.util.Util.createSyncFileName;
import static com.evolveum.polygon.connector.csv.util.Util.handleGenericException;

/**
 * todo check new FileSystem().newWatchService() to create exclusive tmp file https://docs.oracle.com/javase/tutorial/essential/io/notification.html
 * <p>
 * Created by lazyman on 27/01/2017.
 */
public class ObjectClassHandler implements TestOp, SyncOp {

	private static final Log LOG = Log.getLog(ObjectClassHandler.class);
	private static final String ATTR_RAW_JSON = "rawJson";

	private ObjectClassHandlerConfiguration configuration;

	private Map<String, Column> header;

	public ObjectClassHandler(ObjectClassHandlerConfiguration configuration) {
		this.configuration = configuration;

		header = initHeader(configuration.getFilePath());
	}

	private Map<String, Column> initHeader(File csvFile) {
		synchronized (LiveSyncOnlyCsvConnector.SYNCH_FILE_LOCK) {
			CSVFormat csv = Util.createCsvFormat(configuration);
			try (Reader reader = Util.createReader(csvFile, configuration)) {
				CSVParser parser = csv.parse(reader);
				Iterator<CSVRecord> iterator = parser.iterator();

				CSVRecord record = null;
				while (iterator.hasNext()) {
					record = iterator.next();
					if (!isRecordEmpty(record)) {
						break;
					}
				}

				if (record == null) {
					throw new ConfigurationException("Couldn't initialize headers, nothing in csv file for object class "
							+ configuration.getObjectClass());
				}

				return createHeader(record);
			} catch (IOException ex) {
				throw new ConnectorIOException("Couldn't initialize connector for object class "
						+ configuration.getObjectClass(), ex);
			}
		}
	}

	private String getAvailableAttributeName(Map<String, Column> header, String realName) {
		String availableName = realName;
		for (int i = 1; i <= header.size(); i++) {
			if (!header.containsKey(availableName)) {
				break;
			}

			availableName = realName + i;
		}

		return availableName;
	}

	private Map<String, Column> createHeader(CSVRecord record) {
		Map<String, Column> header = new HashMap<>();

		if (configuration.isHeaderExists()) {
			for (int i = 0; i < record.size(); i++) {
				String name = record.get(i);

				if (StringUtil.isEmpty(name)) {
					name = Util.DEFAULT_COLUMN_NAME + 0;
				}

				String availableName = getAvailableAttributeName(header, name);
				header.put(availableName, new Column(name, i));
			}
		} else {
			// header doesn't exist, we just create col0...colN
			for (int i = 0; i < record.size(); i++) {
				header.put(Util.DEFAULT_COLUMN_NAME + i, new Column(null, i));
			}
		}

		LOG.ok("Created header {0}", header);

		testHeader(header);

		return header;
	}

	private void testHeader(Map<String, Column> headers) {
		boolean uniqueFound = false;
		boolean passwordFound = false;

		for (String header : headers.keySet()) {
			if (header.equals(configuration.getUniqueAttribute())) {
				uniqueFound = true;
				continue;
			}

			if (header.equals(configuration.getPasswordAttribute())) {
				passwordFound = true;
				continue;
			}

			if (uniqueFound && passwordFound) {
				break;
			}
		}

		if (!uniqueFound) {
			throw new ConfigurationException("Header in csv file doesn't contain "
					+ "unique attribute name as defined in configuration.");
		}

		if (StringUtil.isNotEmpty(configuration.getPasswordAttribute()) && !passwordFound) {
			throw new ConfigurationException("Header in csv file doesn't contain "
					+ "password attribute name as defined in configuration.");
		}
	}

	public ObjectClass getObjectClass() {
		return configuration.getObjectClass();
	}

	public void schema(SchemaBuilder schema) {
		try {

			ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
			objClassBuilder.setType(getObjectClass().getObjectClassValue());
			objClassBuilder.setAuxiliary(configuration.isAuxiliary());
			objClassBuilder.setContainer(configuration.isContainer());
			objClassBuilder.addAllAttributeInfo(createAttributeInfo(header));

			schema.defineObjectClass(objClassBuilder.build());
		} catch (Exception ex) {
			handleGenericException(ex, "Couldn't initialize connector");
		}
	}

	private List<AttributeInfo> createAttributeInfo(Map<String, Column> columns) {
		List<String> multivalueAttributes = new ArrayList<>();
		if (StringUtil.isNotEmpty(configuration.getMultivalueAttributes())) {
			String[] array = configuration.getMultivalueAttributes().split(configuration.getMultivalueDelimiter());
			multivalueAttributes = Arrays.asList(array);
		}

		List<AttributeInfo> infos = new ArrayList<>();
		for (String name : columns.keySet()) {
			if (name == null || name.isEmpty()) {
				continue;
			}

			if (name.equals(configuration.getUniqueAttribute())) {
				// unique column
				AttributeInfoBuilder builder = new AttributeInfoBuilder(Uid.NAME);
				builder.setType(String.class);
				builder.setNativeName(name);

				infos.add(builder.build());

				if (!isUniqueAndNameAttributeEqual()) {
					builder = new AttributeInfoBuilder(name);
					builder.setType(String.class);
					builder.setNativeName(name);
					builder.setRequired(true);

					infos.add(builder.build());

					continue;
				}
			}

			if (name.equals(configuration.getNameAttribute())) {
				AttributeInfoBuilder builder = new AttributeInfoBuilder(Name.NAME);
				builder.setType(String.class);
				builder.setNativeName(name);

				if (isUniqueAndNameAttributeEqual()) {
					builder.setRequired(true);
				}

				infos.add(builder.build());

				continue;
			}

			if (name.equals(configuration.getPasswordAttribute())) {
				AttributeInfoBuilder builder = new AttributeInfoBuilder(OperationalAttributes.PASSWORD_NAME);
				builder.setType(GuardedString.class);
				builder.setNativeName(name);

				infos.add(builder.build());

				continue;
			}

			AttributeInfoBuilder builder = new AttributeInfoBuilder(name);
			if (name.equals(configuration.getPasswordAttribute())) {
				builder.setType(GuardedString.class);
			} else {
				builder.setType(String.class);
			}
			builder.setNativeName(name);
			if (multivalueAttributes.contains(name)) {
				builder.setMultiValued(true);
			}

			infos.add(builder.build());
		}

		if (configuration.isGroupByEnabled()) {
			AttributeInfoBuilder builder = new AttributeInfoBuilder(ATTR_RAW_JSON);
			builder.setType(String.class);
			builder.setNativeName(ATTR_RAW_JSON);
			builder.setSubtype(AttributeInfo.Subtypes.STRING_JSON);

			infos.add(builder.build());
		}

		return infos;
	}

	private boolean skipRecord(CSVRecord record) {
		if (configuration.isHeaderExists() && record.getRecordNumber() == 1) {
			return true;
		}

		if (isRecordEmpty(record)) {
			return true;
		}

		return false;
	}

	private void handleJustNewToken(SyncToken token, SyncResultsHandler handler) {
		if (!(handler instanceof SyncTokenResultsHandler)) {
			return;
		}

		SyncTokenResultsHandler tokenHandler = (SyncTokenResultsHandler) handler;
		tokenHandler.handleResult(token);
	}

	@Override
	public void sync(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {
		File syncLockFile = Util.createSyncLockFile(configuration);
		FileLock lock = Util.obtainTmpFileLock(syncLockFile);

		try {
			long tokenLongValue = getTokenValue(token);
			LOG.info("Token {0}", tokenLongValue);

			if (tokenLongValue == -1) {
				//token doesn't exist, we only create new sync file - we're synchronizing from now on
				String newToken = createNewSyncFile();
				handleJustNewToken(new SyncToken(newToken), handler);
				LOG.info("Token value was not defined {0}, only creating new sync file, synchronizing from now on.", token);
				return;
			}

			doSync(tokenLongValue, handler);
		} finally {
			Util.closeQuietly(lock);
			syncLockFile.delete();
		}
	}

	private File findOldCsv(long token, String newToken, SyncResultsHandler handler) {
		File oldCsv = Util.createSyncFileName(token, configuration);
		if (!oldCsv.exists()) {
			// we'll try to find first sync file which is newer than token (there's a possibility
			// that we loose some changes this way - same as for example ldap)
			oldCsv = Util.findOldestSyncFile(token, configuration);
			if (oldCsv == null || oldCsv.equals(createSyncFileName(Long.parseLong(newToken), configuration))) {
				// we didn't found any newer file, we should stop and handle this situation as if this
				// is first time we're doing sync operation (like getLatestSyncToken())
				handleJustNewToken(new SyncToken(newToken), handler);
				LOG.info("File for token wasn't found, sync will stop, new token {0} will be returned.", token);
				return null;
			}
		}

		return oldCsv;
	}

	private String calcDigest(File file) {
		try {
			String digest = DigestUtils.sha256Hex(new FileInputStream(file));
			if (StringUtil.isEmpty(digest)) {
				throw new ConnectorIOException("The sync file is empty: " + file.getPath());
			}
			return digest;
		} catch (IOException e) {
			throw new ConnectorIOException("Can't read oldCsv", e);
		}
	}

	private void doSync(long token, SyncResultsHandler handler) {
		String newToken = createNewSyncFile();
		SyncToken newSyncToken = new SyncToken(newToken);

		File newCsv = Util.createSyncFileName(Long.parseLong(newToken), configuration);

		Integer uidIndex = header.get(configuration.getUniqueAttribute()).getIndex();

		File oldCsv = findOldCsv(token, newToken, handler);
		if (oldCsv == null) {
			LOG.warn("Couldn't find old csv file to create diff, continue synchronization.");
		} else {
			String oldDigest = calcDigest(oldCsv);
			String newDigest = calcDigest(newCsv);

			LOG.ok("Comparing files. Old {0} (exists: {1}, size: {2}, digest: {3}) with new {4} (exists: {5}, size: {6}, digest: {7})",
					oldCsv.getName(), oldCsv.exists(), oldCsv.length(), oldDigest, newCsv.getName(), newCsv.exists(), newCsv.length(), newDigest);

			if (oldDigest.equals(newDigest)) {
				// no changes
				LOG.ok("No changes, finishing synchronization.");
				return;
			}
		}

		try (Reader reader = Util.createReader(newCsv, configuration)) {
			CSVFormat csv = Util.createCsvFormatReader(configuration);

			CSVParser parser = csv.parse(reader);
			Iterator<CSVRecord> iterator = parser.iterator();

			int changesCount;
			if (configuration.isGroupByEnabled()) {
				changesCount = doSyncInternalWithGroupBy(handler, iterator, uidIndex, newSyncToken, newCsv.getName());
			} else {
				changesCount = doSyncInternal(handler, iterator, uidIndex, newSyncToken, newCsv.getName());
			}

			if (changesCount == 0) {
				handleJustNewToken(new SyncToken(newToken), handler);
			}
		} catch (Exception ex) {
			handleGenericException(ex, "Error during synchronization");
		} finally {
			cleanupOldSyncFiles();
		}
	}

	private int doSyncInternal(SyncResultsHandler handler, Iterator<CSVRecord> iterator, Integer uidIndex, SyncToken newSyncToken, String csvName) {
		int changesCount = 0;

		boolean shouldContinue = true;
		while (iterator.hasNext()) {
			CSVRecord record = iterator.next();
			if (skipRecord(record)) {
				continue;
			}

			String uid = record.get(uidIndex);
			if (StringUtil.isEmpty(uid)) {
				throw new ConnectorException("Unique attribute not defined for record number "
						+ record.getRecordNumber() + " in " + csvName);
			}

			SyncDelta delta = doSyncCreateOrUpdate(record, uid, newSyncToken, handler);
			if (delta == null) {
				continue;
			}

			changesCount++;
			shouldContinue = handler.handle(delta);
			if (!shouldContinue) {
				break;
			}
		}

		return changesCount;
	}

	private int doSyncInternalWithGroupBy(SyncResultsHandler handler, Iterator<CSVRecord> iterator, Integer uidIndex, SyncToken newSyncToken, String csvName) {
		int changesCount = 0;

		boolean shouldContinue = true;
		List<CSVRecord> recordGroup = new ArrayList<>();

		while (iterator.hasNext()) {
			CSVRecord record = iterator.next();
			if (skipRecord(record)) {
				continue;
			}

			String uid = record.get(uidIndex);
			if (StringUtil.isEmpty(uid)) {
				throw new ConnectorException("Unique attribute not defined for record number "
						+ record.getRecordNumber() + " in " + csvName);
			}

			// Record group is empty, so push it then check next record
			if (recordGroup.isEmpty()) {
				recordGroup.add(record);
				continue;
			}

			// Next record is same uid with the current record group, so push it then check next record
			String currentUid = recordGroup.get(0).get(uidIndex);
			if (uid.equals(currentUid)) {
				recordGroup.add(record);
				continue;
			}

			// Detected different uid record, create delta
			SyncDelta delta = doSyncCreateOrUpdate(recordGroup, newSyncToken);

			// Reset record group for next
			recordGroup.clear();
			recordGroup.add(record);

			changesCount++;
			shouldContinue = handler.handle(delta);
			if (!shouldContinue) {
				break;
			}
		}

		// Handle remains
		if (shouldContinue && !recordGroup.isEmpty()) {
			SyncDelta delta = doSyncCreateOrUpdate(recordGroup, newSyncToken);
			changesCount++;
			handler.handle(delta);
		}

		return changesCount;
	}

	private void cleanupOldSyncFiles() {
		String[] tokenFiles = Util.listTokenFiles(configuration);
		Arrays.sort(tokenFiles);

		int preserve = configuration.getPreserveOldSyncFiles();
		if (preserve <= 1) {
			LOG.info("Not removing old token files. Preserve last tokens: {0}.", preserve);
			return;
		}

		File parentFolder = configuration.getTmpFolder();
		for (int i = 0; i + preserve < tokenFiles.length; i++) {
			File tokenSyncFile = new File(parentFolder, tokenFiles[i]);
			if (!tokenSyncFile.exists()) {
				continue;
			}

			LOG.info("Deleting file {0}.", tokenSyncFile.getName());
			tokenSyncFile.delete();
		}
	}

	private SyncDelta doSyncCreateOrUpdate(CSVRecord newRecord, String newRecordUid, SyncToken newSyncToken, SyncResultsHandler handler) {
		SyncDelta delta;

		// TODO Need to distinguish create or update?
		delta = buildSyncDelta(SyncDeltaType.CREATE_OR_UPDATE, newSyncToken, newRecord);

		LOG.ok("Created delta {0}", delta);

		return delta;
	}

	private SyncDelta doSyncCreateOrUpdate(List<CSVRecord> recordGroup, SyncToken newSyncToken) {
		SyncDeltaBuilder builder = new SyncDeltaBuilder();
		builder.setDeltaType(SyncDeltaType.CREATE_OR_UPDATE);
		builder.setObjectClass(ObjectClass.ACCOUNT);
		builder.setToken(newSyncToken);

		Map<Integer, String> header = reverseHeaderMap();

		// Create base connectorObject using first record
		ConnectorObjectBuilder connectorObjectBuilder = createConnectorObjectBuilder(recordGroup.get(0));

		// Then, create rawJson and append to the connectorObject
		List<Map<String, String>> data = new ArrayList<>();
		for (CSVRecord record : recordGroup) {
			if (header.size() != record.size()) {
				throw new ConnectorException("Number of columns in header (" + header.size()
						+ ") doesn't match number of columns for record (" + record.size()
						+ "). File row number: " + record.getRecordNumber());
			}

			Map<String, String> recordMap = new HashMap<>();

			for (int i = 0; i < record.size(); i++) {
				String name = header.get(i);
				String value = record.get(i);

				if (StringUtil.isEmpty(value)) {
					// Include empty string for JSON if no value
					recordMap.put(name, "");
				} else {
					recordMap.put(name, value);
				}
			}

			if (!recordMap.isEmpty()) {
				data.add(recordMap);
			}
		}
		if (!data.isEmpty()) {
			String json = new Gson().toJson(data);
			connectorObjectBuilder.addAttribute(ATTR_RAW_JSON, json);
		}

		ConnectorObject connectorObject = connectorObjectBuilder.build();
		builder.setObject(connectorObject);

		SyncDelta delta = builder.build();

		LOG.ok("Created delta {0}", delta);

		return delta;
	}

	private SyncDelta buildSyncDelta(SyncDeltaType type, SyncToken token, CSVRecord record) {
		SyncDeltaBuilder builder = new SyncDeltaBuilder();
		builder.setDeltaType(type);
		builder.setObjectClass(ObjectClass.ACCOUNT);
		builder.setToken(token);

		ConnectorObject object = createConnectorObject(record);
		builder.setObject(object);

		return builder.build();
	}

	private long getTokenValue(SyncToken token) {
		if (token == null || token.getValue() == null) {
			return -1;
		}
		String object = token.getValue().toString();
		if (!object.matches("[0-9]{13}")) {
			return -1;
		}

		return Long.parseLong(object);
	}

	private String createNewSyncFile() {
		long timestamp = System.currentTimeMillis();

		String token = null;
		try {
			File real = configuration.getFilePath();

			File last = Util.createSyncFileName(timestamp, configuration);

			LOG.info("Creating new sync file {0} file {1}", timestamp, last.getName());
			Files.copy(real.toPath(), last.toPath(), StandardCopyOption.REPLACE_EXISTING);
			LOG.ok("New sync file created, name {0}, size {1}", last.getName(), last.length());

			token = Long.toString(timestamp);
		} catch (IOException ex) {
			handleGenericException(ex, "Error occurred while creating new sync file " + timestamp);
		}

		return token;
	}

	@Override
	public SyncToken getLatestSyncToken(ObjectClass oc) {
		String token;
		LOG.info("Creating token, synchronizing from \"now\".");
		token = createNewSyncFile();

		return new SyncToken(token);
	}

	@Override
	public void test() {
		configuration.validate();

		initHeader(configuration.getFilePath());
	}

	private boolean isRecordEmpty(CSVRecord record) {
		if (!configuration.isIgnoreEmptyLines()) {
			return false;
		}

		for (int i = 0; i < record.size(); i++) {
			String value = record.get(i);
			if (StringUtil.isNotBlank(value)) {
				return false;
			}
		}

		return true;
	}

	private Map<Integer, String> reverseHeaderMap() {
		Map<Integer, String> reversed = new HashMap<>();
		this.header.forEach((key, value) -> {

			reversed.put(value.getIndex(), key);
		});

		return reversed;
	}

	private ConnectorObject createConnectorObject(CSVRecord record) {
		return createConnectorObjectBuilder(record).build();
	}

	private ConnectorObjectBuilder createConnectorObjectBuilder(CSVRecord record) {
		ConnectorObjectBuilder builder = new ConnectorObjectBuilder();

		Map<Integer, String> header = reverseHeaderMap();

		if (header.size() != record.size()) {
			throw new ConnectorException("Number of columns in header (" + header.size()
					+ ") doesn't match number of columns for record (" + record.size()
					+ "). File row number: " + record.getRecordNumber());
		}

		for (int i = 0; i < record.size(); i++) {
			String name = header.get(i);
			String value = record.get(i);

			if (StringUtil.isEmpty(value)) {
				continue;
			}

			if (name.equals(configuration.getUniqueAttribute())) {
				builder.setUid(value);

				if (!isUniqueAndNameAttributeEqual()) {
					continue;
				}
			}

			if (name.equals(configuration.getNameAttribute())) {
				builder.setName(new Name(value));
				continue;
			}

			if (name.equals(configuration.getPasswordAttribute())) {
				builder.addAttribute(OperationalAttributes.PASSWORD_NAME, new GuardedString(value.toCharArray()));
				continue;
			}

			builder.addAttribute(name, createAttributeValues(value));
		}

		return builder;
	}

	private boolean isUniqueAndNameAttributeEqual() {
		String uniqueAttribute = configuration.getUniqueAttribute();
		String nameAttribute = configuration.getNameAttribute();

		return uniqueAttribute == null ? nameAttribute == null : uniqueAttribute.equals(nameAttribute);
	}

	private List<String> createAttributeValues(String attributeValue) {
		List<String> values = new ArrayList<>();

		if (StringUtil.isEmpty(configuration.getMultivalueDelimiter())) {
			values.add(attributeValue);
		} else {
			String[] array = attributeValue.split(configuration.getMultivalueDelimiter());
			for (String item : array) {
				if (StringUtil.isEmpty(item)) {
					continue;
				}

				values.add(item);
			}
		}

		return values;
	}

	private boolean isName(String column) {
		return configuration.getNameAttribute().equals(column);
	}
}
