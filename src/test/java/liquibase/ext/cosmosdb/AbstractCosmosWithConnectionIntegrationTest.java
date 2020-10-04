package liquibase.ext.cosmosdb;

/*-
 * #%L
 * Liquibase MongoDB Extension
 * %%
 * Copyright (C) 2019 Mastercard
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


import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import liquibase.Scope;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.executor.ExecutorService;
import liquibase.ext.cosmosdb.database.CosmosConnection;
import liquibase.ext.cosmosdb.database.CosmosLiquibaseDatabase;
import liquibase.lockservice.LockServiceFactory;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/**
 * With {@link CosmosLiquibaseDatabase} initiated
 */
public abstract class AbstractCosmosWithConnectionIntegrationTest extends AbstractCosmosIntegrationTest {

    protected CosmosLiquibaseDatabase cosmosLiquibaseDatabase;
    protected CosmosConnection cosmosConnection;
    protected CosmosDatabase cosmosDatabase;


    @BeforeAll
    protected void setUp() {
        super.setUp();

    }

    @BeforeEach
    @SneakyThrows
    protected void setUpEach() {
        resetServices();
        cosmosConnection = new CosmosConnection();
        cosmosConnection.open(connectionString, driver, driverProperties);
        cosmosLiquibaseDatabase = new CosmosLiquibaseDatabase();
        cosmosLiquibaseDatabase.setConnection(cosmosConnection);
        cosmosDatabase = cosmosConnection.getCosmosDatabase();
        deleteContainers();
    }

    @AfterEach
    @SneakyThrows
    protected void tearDownEach() {
        deleteContainers();
        resetServices();
        cosmosConnection.close();
    }

    protected void deleteContainers() {
        cosmosDatabase.readAllContainers().stream()
                .map(CosmosContainerProperties::getId).map(cosmosDatabase::getContainer).forEach(CosmosContainer::delete);
    }

    protected void resetServices() {
        LockServiceFactory.getInstance().resetAll();
        ChangeLogHistoryServiceFactory.getInstance().resetAll();
        Scope.getCurrentScope().getSingleton(ExecutorService.class).reset();
    }

}
