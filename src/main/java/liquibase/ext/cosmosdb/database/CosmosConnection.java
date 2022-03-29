package liquibase.ext.cosmosdb.database;

/*-
 * #%L
 * Liquibase CosmosDB Extension
 * %%
 * Copyright (C) 2020 Mastercard
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.azure.cosmos.CosmosDatabase;
import liquibase.exception.DatabaseException;
import liquibase.nosql.database.AbstractNoSqlConnection;
import liquibase.util.StringUtil;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.Driver;
import java.util.Properties;

import static java.util.Objects.isNull;
import static java.util.Optional.ofNullable;
import static liquibase.ext.cosmosdb.database.CosmosConnectionString.*;

@Getter
@Setter
@NoArgsConstructor()
public class CosmosConnection extends AbstractNoSqlConnection {

    public static final String LIQUIBASE_EXTENSION_USER_AGENT_SUFFIX = "LiquibaseExtension";

    private CosmosConnectionString cosmosConnectionString;

    private CosmosClientProxy cosmosClient;

    private CosmosDatabase cosmosDatabase;

    @Override
    public String getCatalog() throws DatabaseException {
        return this.cosmosConnectionString.getDatabaseName().orElse("");
    }

    @Override
    public String getDatabaseProductName() throws DatabaseException {
        //TODO: refer to metadata
        return CosmosLiquibaseDatabase.COSMOSDB_PRODUCT_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws DatabaseException {
        //TODO: refer to metadata
        return "3.12.7";
    }

    @Override
    public int getDatabaseMajorVersion() throws DatabaseException {
        //TODO: refer to metadata
        return 3;
    }

    @Override
    public int getDatabaseMinorVersion() throws DatabaseException {
        //TODO: refer to metadata
        return 12;
    }

    @Override
    public String getURL() {
        return ofNullable(cosmosConnectionString)
                .map(CosmosConnectionString::toUrl).orElse("");
    }

    @Override
    public String getConnectionUserName() {
        return ofNullable(this.cosmosConnectionString)
                .flatMap(CosmosConnectionString::getAccountEndpoint).orElse("");
    }

    @Override
    public boolean isClosed() throws DatabaseException {
        return isNull(this.cosmosClient) || isNull(this.cosmosDatabase);
    }

    /**
     * Opens a CosmosConnection based
     * Creates a new client with the given connection string.
     * Creates a database if not exists with the DatabaseName passed via {@link CosmosConnectionString#fromJsonConnectionString(String)}
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param url              connectionString the json format connection string
     * @param driverObject     driverObject identified
     * @param driverProperties driverProperties passed through
     * @see CosmosConnectionString
     */
    @Override
    public void open(final String url, final Driver driverObject, final Properties driverProperties) throws DatabaseException {
        open(fromConnectionString(url), driverObject, driverProperties);
    }

    public void open(final CosmosConnectionString cosmosConnectionString, final Driver driverObject, final Properties driverProperties) throws DatabaseException {

        this.cosmosConnectionString = cosmosConnectionString;

        if (!cosmosConnectionString.getAccountEndpoint().isPresent()
                || !cosmosConnectionString.getAccountKey().isPresent()
                || !cosmosConnectionString.getDatabaseName().isPresent()) {
            throw new IllegalArgumentException(String.format("Missing one of the properties [%s, %s, %s]"
                    , ACCOUNT_ENDPOINT_PROPERTY, ACCOUNT_KEY_PROPERTY, DATABASE_NAME_PROPERTY));
        }

        this.cosmosClient = ((CosmosClientDriver) driverObject).connect(cosmosConnectionString);

        final String databaseName = cosmosConnectionString.getDatabaseName().get();

        try {
            this.cosmosClient.createDatabaseIfNotExists(databaseName);
            this.cosmosDatabase = this.cosmosClient.getDatabase(databaseName);
        } catch (final Exception e) {
            throw new DatabaseException("Could not create database: " + databaseName, e);
        }
    }

    @Override
    public boolean supports(String url) {
        return !StringUtil.isEmpty(StringUtil.trimToNull(url)) && url.startsWith(COSMOSDB_PREFIX);
    }

    @Override
    public void close() throws DatabaseException {
        try {
            if (!isClosed()) {
                this.cosmosClient.close();
                reset();
            }
        } catch (final Exception e) {
            throw new DatabaseException(e);
        }
    }

    private void reset() {
        this.cosmosClient = null;
        this.cosmosDatabase = null;
        this.cosmosConnectionString = null;
    }

}
