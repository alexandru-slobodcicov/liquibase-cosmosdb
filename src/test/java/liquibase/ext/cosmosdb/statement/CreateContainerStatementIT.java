package liquibase.ext.cosmosdb.statement;

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosException;
import liquibase.ext.cosmosdb.AbstractCosmosWithConnectionIntegrationTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class CreateContainerStatementIT extends AbstractCosmosWithConnectionIntegrationTest {

    public static final String CONTAINER_NAME_1 = "containerName1";
    public static final String PARTITION_KEY_PATH_1 = "{ \"partitionKey\": {\"paths\": [\"/partitionField1\"], \"kind\": \"Hash\" } }";


    @SneakyThrows
    @Test
    void testExecute() {
        final CreateContainerStatement createContainerStatement
                = new CreateContainerStatement(CONTAINER_NAME_1, PARTITION_KEY_PATH_1);

        createContainerStatement.execute(cosmosDatabase);

        final CosmosContainer cosmosContainer = cosmosDatabase.getContainer(CONTAINER_NAME_1);

        assertThat(cosmosContainer).isNotNull();
        assertThat(cosmosContainer.getId()).isEqualTo(CONTAINER_NAME_1);

        assertThatExceptionOfType(CosmosException.class).isThrownBy(() -> createContainerStatement.execute(cosmosDatabase));

        assertThat(cosmosDatabase.getContainer("testcoll")).isNotNull();

    }
}