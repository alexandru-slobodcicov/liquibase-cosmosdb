package liquibase.ext.cosmosdb.statement;

import com.azure.cosmos.CosmosContainer;
import liquibase.ext.cosmosdb.AbstractCosmosWithConnectionIntegrationTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class CreateContainerIfNotExistsStatementIT extends AbstractCosmosWithConnectionIntegrationTest {
    public static final String CONTAINER_NAME_1 = "containerName1";
    public static final String PARTITION_KEY_PATH_1 = "{ \"partitionKey\": {\"paths\": [\"/partitionField1\"], \"kind\": \"Hash\" } }";

    @SneakyThrows
    @Test
    void testExecute() {
        final CreateContainerStatement createContainerIfNotExistsStatement
                = new CreateContainerIfNotExistsStatement(CONTAINER_NAME_1, PARTITION_KEY_PATH_1);

        createContainerIfNotExistsStatement.execute(cosmosDatabase);

        final CosmosContainer cosmosContainer = cosmosDatabase.getContainer(CONTAINER_NAME_1);

        assertThat(cosmosContainer).isNotNull();
        assertThat(cosmosContainer.getId()).isEqualTo(CONTAINER_NAME_1);

        assertThatNoException().isThrownBy(() -> createContainerIfNotExistsStatement.execute(cosmosDatabase));
    }
}