package liquibase.ext.cosmosdb.statement;

import liquibase.ext.cosmosdb.AbstractCosmosWithConnectionIntegrationTest;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CountContainersByNameStatementIT extends AbstractCosmosWithConnectionIntegrationTest {

    private static final String CONTAINER_NAME_1 = "containerName1";
    private static final String CONTAINER_NAME_2 = "containerName2";

    @SneakyThrows
    @Test
    void testQueryForLong() {

        final CountContainersByNameStatement countContainersByNameStatement
                = new CountContainersByNameStatement(CONTAINER_NAME_1);

        final Long countBeforeCreated = countContainersByNameStatement.queryForLong(cosmosDatabase);
        assertThat(countBeforeCreated).isEqualTo(0L);

        final CreateContainerStatement createContainerStatement1
                = new CreateContainerStatement(CONTAINER_NAME_1);

        final CreateContainerStatement createContainerStatement2
                = new CreateContainerStatement(CONTAINER_NAME_2);

        createContainerStatement1.execute(cosmosDatabase);
        createContainerStatement2.execute(cosmosDatabase);

        final Long countAfterCreated = countContainersByNameStatement.queryForLong(cosmosDatabase);
        assertThat(countAfterCreated).isEqualTo(1L);

    }

}