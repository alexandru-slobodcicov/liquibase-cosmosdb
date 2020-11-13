package liquibase.ext.cosmosdb.database;

import liquibase.ext.cosmosdb.AbstractCosmosIntegrationTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CosmosConnectionIT extends AbstractCosmosIntegrationTest {

    @SneakyThrows
    @Test
    void openTest() {
        CosmosConnection connection = new CosmosConnection();
        assertThat(connection.getCosmosClient()).isNull();
        assertThat(connection.getCosmosDatabase()).isNull();
        assertThat(connection.isClosed()).isTrue();

        connection.open(connectionString, driver, driverProperties);
        assertThat(connection.getCosmosClient()).isNotNull();
        assertThat(connection.getCosmosDatabase()).isNotNull();
        assertThat(connection.isClosed()).isFalse();
    }

}