package com.evolveum.polygon.connector.csv.util;

import com.evolveum.polygon.connector.csv.CsvConfiguration;
import com.evolveum.polygon.connector.csv.LiveSyncOnlyCsvConnector;
import com.evolveum.polygon.connector.csv.ObjectClassHandlerConfiguration;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by Viliam Repan (lazyman).
 */
public class Util {

    private static final Log LOG = Log.getLog(Util.class);

    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");

    public static final String TMP_EXTENSION = "tmp";

    public static final String SYNC_LOCK_EXTENSION = "sync.lock";

    public static final String DEFAULT_COLUMN_NAME = "col";

    public static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }

        try {
            closeable.close();
        } catch (IOException ex) {
        }
    }

    public static void closeQuietly(FileLock lock) {
        try {
            if (lock != null && lock.isValid()) {
                lock.channel().close(); // channel must be close to avoid fd leak (too many open files)
            }
        } catch (IOException ex) {
            LOG.warn("Unlock failed for {0}!", lock);
        }
    }

    public static File createSyncLockFile(ObjectClassHandlerConfiguration config) {
        String fileName = config.getFilePath().getName() + "." + SYNC_LOCK_EXTENSION;
        return new File(config.getTmpFolder(), fileName);
    }

    public static File createTmpPath(ObjectClassHandlerConfiguration config) {
        String fileName = config.getFilePath().getName() + config.hashCode() + "." + TMP_EXTENSION;
        return new File(config.getTmpFolder(), fileName);
    }

    public static FileLock obtainTmpFileLock(ObjectClassHandlerConfiguration config) {
        File tmp = createTmpPath(config);

        return obtainTmpFileLock(tmp);
    }

    public static FileLock obtainTmpFileLock(File file) {
        LOG.ok("Obtaining file lock for {0}", file.getPath());

        int attempts = 0;

        final long MAX_WAIT = 5 * 1000; // 5 seconds
        Path path = file.toPath();

        long start = System.currentTimeMillis();
        FileChannel channel;
        while (true) {
            try {
                attempts++;

                channel = FileChannel.open(path,
                        new HashSet(Arrays.asList(StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)));

                break;
            } catch (IOException ex) {
                if (System.currentTimeMillis() > (start + MAX_WAIT)) {
                    throw new ConnectorIOException("Timeout, couldn't create tmp file '" + file.getPath()
                            + "', reason: " + ex.getMessage(), ex);
                }

                try {
                    Thread.sleep((long) (10 + (Math.random() * 50)));
                } catch (InterruptedException ie) {
                    throw new ConnectorException(ie);
                }
            }
        }

        FileLock lock;
        try {
            lock = channel.lock();
        } catch (IOException ex) {
            try {
                channel.close();
            } catch (IOException io) {
                LOG.warn("Close failed for {0}!", channel);
            }

            if (!file.delete()) {
                throw new ConnectorIOException("Couldn't delete lock file '" + file.getPath() + "'");
            }

            throw new ConnectorIOException("Couldn't obtain lock for temp file '" + file.getPath()
                    + "', reason: " + ex.getMessage(), ex);
        }

        LOG.ok("Lock for file {0} obtained (attempts: {1})", file.getPath(), attempts);

        return lock;
    }

    public static <T> T getSafeValue(Map<String, Object> map, String key, T defValue) {
        return (T) getSafeValue(map, key, defValue, (Class) String.class);
    }

    public static <T> T getSafeValue(Map<String, Object> map, String key, T defValue, Class<T> type) {
        if (map == null) {
            return defValue;
        }

        Object value = map.get(key);
        if (value == null) {
            return defValue;
        }

        String strValue = value.toString();
        if (String.class.equals(type)) {
            return (T) strValue;
        } else if (Integer.class.equals(type)) {
            return (T) Integer.valueOf(strValue);
        } else if (Boolean.class.equals(type)) {
            return (T) Boolean.valueOf(strValue);
        } else if (File.class.equals(type)) {
            return (T) new File(strValue);
        }

        return defValue;
    }

    public static BufferedReader createReader(File path, ObjectClassHandlerConfiguration configuration) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        InputStreamReader in = new InputStreamReader(fis, configuration.getEncoding());
        return new BufferedReader(in);
    }

    public static void checkCanReadFile(File file) {
        if (file == null) {
            throw new ConfigurationException("File path is not defined");
        }
        
        synchronized (LiveSyncOnlyCsvConnector.SYNCH_FILE_LOCK) {
        	if (!file.exists()) {
        		throw new ConfigurationException("File '" + file + "' doesn't exists. At least file with CSV header must exist");
        	}
        	if (file.isDirectory()) {
        		throw new ConfigurationException("File path '" + file + "' is a directory, must be a CSV file");
        	}
        	if (!file.canRead()) {
        		throw new ConfigurationException("File '" + file + "' can't be read");
        	}
        }
    }

    public static void handleGenericException(Exception ex, String message) {
        if (ex instanceof IllegalArgumentException) {
            throw (IllegalArgumentException) ex;
        }

        if (ex instanceof ConnectorException) {
            throw (ConnectorException) ex;
        }

        if (ex instanceof IOException) {
            throw new ConnectorIOException(message + ", IO exception occurred, reason: " + ex.getMessage(), ex);
        }

        throw new ConnectorException(message + ", reason: " + ex.getMessage(), ex);
    }

    public static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public static BufferedReader createReader(CsvConfiguration configuration) throws IOException {
        return createReader(configuration.getFilePath(), configuration);
    }

    public static BufferedReader createReader(File path, CsvConfiguration configuration) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        InputStreamReader in = new InputStreamReader(fis, configuration.getEncoding());
        return new BufferedReader(in);
    }

    public static Character toCharacter(String value) {
        if (value == null) {
            return null;
        }

        if (value.length() != 1) {
            throw new ConfigurationException("Can't cast to character, illegal string size: "
                    + value.length() + ", should be 1");
        }

        return value.charAt(0);
    }

    public static void notEmpty(String str, String message) {
        if (StringUtil.isEmpty(str)) {
            throw new ConfigurationException(message);
        }
    }

    public static CSVFormat createCsvFormatReader(ObjectClassHandlerConfiguration configuration) {
        CSVFormat format = createCsvFormat(configuration);
        format = format.withSkipHeaderRecord(configuration.isHeaderExists());

        return format;
    }

    public static CSVFormat createCsvFormat(ObjectClassHandlerConfiguration configuration) {
        notNull(configuration, "CsvConfiguration must not be null");

        return CSVFormat.newFormat(toCharacter(configuration.getFieldDelimiter()))
                .withAllowMissingColumnNames(false)
                .withEscape(toCharacter(configuration.getEscape()))
                .withCommentMarker(toCharacter(configuration.getCommentMarker()))
                .withIgnoreEmptyLines(configuration.isIgnoreEmptyLines())
                .withIgnoreHeaderCase(false)
                .withIgnoreSurroundingSpaces(configuration.isIgnoreSurroundingSpaces())
                .withQuote(toCharacter(configuration.getQuote()))
                .withQuoteMode(QuoteMode.valueOf(configuration.getQuoteMode()))
                .withRecordSeparator(configuration.getRecordSeparator())
                .withTrailingDelimiter(configuration.isTrailingDelimiter())
                .withTrim(configuration.isTrim());
    }

    public static String[] listTokenFiles(ObjectClassHandlerConfiguration config) {
        File csv = config.getFilePath();

        File tmpFolder = config.getTmpFolder();

        String csvFileName = csv.getName();
        return tmpFolder.list(new SyncTokenFileFilter(csvFileName));
    }

    public static File createSyncFileName(long timestamp, ObjectClassHandlerConfiguration config) {
        File csv = config.getFilePath();
        String fileName = csv.getName();

        File tmpFolder = config.getTmpFolder();

        return new File(tmpFolder, fileName + ".sync." + timestamp);
    }

    public static File findOldestSyncFile(long token, ObjectClassHandlerConfiguration config) {
        String[] tokenFiles = Util.listTokenFiles(config);
        Arrays.sort(tokenFiles);

        for (String name : tokenFiles) {
            String[] array = name.split("\\.");
            String fileToken = array[array.length - 1];

            long fileTokenLong = Long.parseLong(fileToken);
            if (fileTokenLong <= token) {
                continue;
            }

            File tmpFolder = config.getTmpFolder();
            return new File(tmpFolder, name);
        }

        return null;
    }
}
