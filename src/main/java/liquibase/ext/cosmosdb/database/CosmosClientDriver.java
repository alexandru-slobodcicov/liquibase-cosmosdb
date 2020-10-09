package liquibase.ext.cosmosdb.database;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogService;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.util.Properties;
import java.util.logging.Logger;

import static liquibase.ext.cosmosdb.database.CosmosConnection.LIQUIBASE_EXTENSION_USER_AGENT_SUFFIX;
import static org.apache.commons.lang3.StringUtils.EMPTY;

public class CosmosClientDriver implements Driver {

    @Override
    public Connection connect(final String url, final Properties info) {
        //Not applicable for non JDBC DBs
        throw new UnsupportedOperationException("Cannot initiate a SQL Connection for a NoSql DB");
    }

    public CosmosClientProxy connect(final CosmosConnectionString cosmosConnectionString) throws DatabaseException {
        final CosmosClient client;
        try {
            client = new CosmosClientBuilder()
                    .endpoint(cosmosConnectionString.getAccountEndpoint().orElse(EMPTY))
                    .key(cosmosConnectionString.getAccountKey().orElse(EMPTY))
                    .consistencyLevel(ConsistencyLevel.EVENTUAL)
                    .userAgentSuffix(LIQUIBASE_EXTENSION_USER_AGENT_SUFFIX)
                    .buildClient();
        } catch (final Exception e) {
            throw new DatabaseException("Connection could not be established to: "
                    + cosmosConnectionString.getConnectionString(), e);
        }
        return CosmosClientProxy.builder().cosmosClient(client).build();
    }

    @Override
    public boolean acceptsURL(final String url) {
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return (Logger) LogService.getLog(getClass());
    }
}
