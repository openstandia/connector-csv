package com.evolveum.polygon.connector.csv;

import com.evolveum.polygon.connector.csv.util.Util;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Viliam Repan (lazyman).
 */
@ConnectorClass(
        displayNameKey = "UI_CSV_CONNECTOR_NAME",
        configurationClass = CsvConfiguration.class)
public class LiveSyncOnlyCsvConnector implements Connector, TestOp, SchemaOp, SyncOp {

	public static final Integer SYNCH_FILE_LOCK = 0;
	
    private static final Log LOG = Log.getLog(LiveSyncOnlyCsvConnector.class);

    private CsvConfiguration configuration;

    private Map<ObjectClass, ObjectClassHandler> handlers = new HashMap<>();

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        LOG.info(">>> Initializing connector");

        if (!(configuration instanceof CsvConfiguration)) {
            throw new ConfigurationException("Configuration is not instance of " + CsvConfiguration.class.getName());
        }

        CsvConfiguration csvConfig = (CsvConfiguration) configuration;
        csvConfig.validate();

        this.configuration = csvConfig;

        try {
            List<ObjectClassHandlerConfiguration> configs = this.configuration.getAllConfigs();
            configs.forEach(config -> handlers.put(config.getObjectClass(), new ObjectClassHandler(config)));
        } catch (Exception ex) {
            Util.handleGenericException(ex, "Couldn't initialize connector");
        }

        LOG.info(">>> Connector initialization finished");
    }

    @Override
    public void dispose() {
        configuration = null;
        handlers = null;
    }

    private ObjectClassHandler getHandler(ObjectClass oc) {
        ObjectClassHandler handler = handlers.get(oc);
        if (handler == null) {
            throw new ConnectorException("Unknown object class " + oc);
        }

        return handler;
    }

    @Override
    public Schema schema() {
        LOG.info(">>> schema started");

        SchemaBuilder builder = new SchemaBuilder(LiveSyncOnlyCsvConnector.class);
        handlers.values().forEach(handler -> {

            LOG.info("schema started for {0}", handler.getObjectClass());

            handler.schema(builder);

            LOG.info("schema finished for {0}", handler.getObjectClass());
        });

        Schema schema = builder.build();
        LOG.info(">>> schema finished");

        return schema;
    }

    @Override
    public void sync(ObjectClass oc, SyncToken token, SyncResultsHandler handler, OperationOptions oo) {
        LOG.info(">>> sync {0} {1} {2} {3}", oc, token, handler, oo);

        getHandler(oc).sync(oc, token, handler, oo);

        LOG.info(">>> sync finished");
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass oc) {
        LOG.info(">>> getLatestSyncToken {0}", oc);

        SyncToken token = getHandler(oc).getLatestSyncToken(oc);

        LOG.info(">>> getLatestSyncToken finished");

        return token;
    }

    @Override
    public void test() {
        LOG.info(">>> test started");

        handlers.values().forEach(handler -> {

            LOG.info("test started for {0}", handler.getObjectClass());

            handler.test();

            LOG.info("test finished for {0}", handler.getObjectClass());

        });

        LOG.info(">>> test finished");
    }
}
