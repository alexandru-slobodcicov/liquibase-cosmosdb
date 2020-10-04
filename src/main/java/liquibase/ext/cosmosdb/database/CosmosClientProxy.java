package liquibase.ext.cosmosdb.database;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.io.Closeable;

/**
 * Proxy class which delegates method calls to client {@link CosmosClient}.
 * <p>
 * It is required in order to stub and do not depend on implementation
 */
@AllArgsConstructor
@Builder
public class CosmosClientProxy implements Closeable {
    
    @Getter
    private final CosmosClient cosmosClient;

    /**
     * Create a Cosmos database if it does not already exist on the service.
     * <p>
     * The throughputProperties will only be used if the specified database
     * does not exist and therefore a new database will be created with throughputProperties.
     *
     * @param id                   the id of the database.
     * @param throughputProperties the throughputProperties.
     * @return the {@link CosmosDatabaseResponse} with the created database.
     */
    public CosmosDatabaseResponse createDatabaseIfNotExists(String id, ThroughputProperties throughputProperties) {
        return cosmosClient.createDatabaseIfNotExists(id, throughputProperties);
    }

    /**
     * Create a Cosmos database if it does not already exist on the service.
     *
     * @param id the id of the database.
     * @return the {@link CosmosDatabaseResponse} with the created database.
     */
    public CosmosDatabaseResponse createDatabaseIfNotExists(String id) {
        return cosmosClient.createDatabaseIfNotExists(id);
    }

    /**
     * Creates a database.
     *
     * @param databaseProperties {@link CosmosDatabaseProperties} the database properties.
     * @param options            the request options.
     * @return the {@link CosmosDatabaseResponse} with the created database.
     */
    public CosmosDatabaseResponse createDatabase(CosmosDatabaseProperties databaseProperties,
                                                 CosmosDatabaseRequestOptions options) {
        return cosmosClient.createDatabase(databaseProperties, options);
    }

    /**
     * Creates a Cosmos database.
     *
     * @param databaseProperties {@link CosmosDatabaseProperties} the database properties.
     * @return the {@link CosmosDatabaseResponse} with the created database.
     */
    public CosmosDatabaseResponse createDatabase(CosmosDatabaseProperties databaseProperties) {
        return cosmosClient.createDatabase(databaseProperties);
    }

    /**
     * Creates a Cosmos database.
     *
     * @param id the id of the database.
     * @return the {@link CosmosDatabaseResponse} with the created database.
     */
    public CosmosDatabaseResponse createDatabase(String id) {
        return cosmosClient.createDatabase(id);

    }

    /**
     * Creates a Cosmos database.
     *
     * @param databaseProperties   {@link CosmosDatabaseProperties} the database properties.
     * @param throughputProperties the throughput properties.
     * @param options              {@link CosmosDatabaseRequestOptions} the request options.
     * @return the {@link CosmosDatabaseResponse} with the created database.
     */
    public CosmosDatabaseResponse createDatabase(CosmosDatabaseProperties databaseProperties,
                                                 ThroughputProperties throughputProperties,
                                                 CosmosDatabaseRequestOptions options) {
        return cosmosClient.createDatabase(databaseProperties, throughputProperties, options);
    }

    /**
     * Creates a Cosmos database.
     *
     * @param databaseProperties   {@link CosmosDatabaseProperties} the database properties.
     * @param throughputProperties the throughput properties.
     * @return the {@link CosmosDatabaseResponse} with the created database.
     */
    public CosmosDatabaseResponse createDatabase(CosmosDatabaseProperties databaseProperties,
                                                 ThroughputProperties throughputProperties) {
        return cosmosClient.createDatabase(databaseProperties, throughputProperties);
    }

    /**
     * Creates a Cosmos database.
     *
     * @param id                   the id of the database.
     * @param throughputProperties the throughput properties.
     * @return the {@link CosmosDatabaseResponse} with the created database.
     */
    public CosmosDatabaseResponse createDatabase(String id, ThroughputProperties throughputProperties) {
        return cosmosClient.createDatabase(id, throughputProperties);
    }

    /**
     * Reads all Cosmos databases.
     *
     * @return the {@link CosmosPagedIterable} for feed response with the read databases.
     */
    public CosmosPagedIterable<CosmosDatabaseProperties> readAllDatabases() {
        return cosmosClient.readAllDatabases();
    }

    /**
     * Query a Cosmos database.
     *
     * @param query   the query.
     * @param options {@link CosmosQueryRequestOptions}the feed options.
     * @return the {@link CosmosPagedIterable} for feed response with the obtained databases.
     */
    public CosmosPagedIterable<CosmosDatabaseProperties> queryDatabases(String query, CosmosQueryRequestOptions options) {
        return cosmosClient.queryDatabases(query, options);
    }

    /**
     * Query a Cosmos database.
     *
     * @param querySpec {@link SqlQuerySpec} the query spec.
     * @param options   the query request options.
     * @return the {@link CosmosPagedIterable} for feed response with the obtained databases.
     */
    public CosmosPagedIterable<CosmosDatabaseProperties> queryDatabases(SqlQuerySpec querySpec,
                                                                        CosmosQueryRequestOptions options) {
        return cosmosClient.queryDatabases(querySpec, options);
    }

    /**
     * Gets the Cosmos database client.
     *
     * @param id the id of the database.
     * @return {@link CosmosDatabase} the cosmos sync database.
     */
    public CosmosDatabase getDatabase(String id) {
        return cosmosClient.getDatabase(id);
    }

    /**
     * Close this {@link com.azure.cosmos.CosmosClient} instance.
     */
    public void close() {
        cosmosClient.close();
    }

}