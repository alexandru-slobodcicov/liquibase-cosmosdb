package liquibase.ext.cosmosdb.changelog;

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

import liquibase.Liquibase;
import liquibase.Scope;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.RanChangeSet;
import liquibase.database.core.H2Database;
import liquibase.executor.ExecutorService;
import liquibase.ext.cosmosdb.AbstractCosmosWithConnectionIntegrationTest;
import liquibase.ext.cosmosdb.executor.CosmosExecutor;
import liquibase.ext.cosmosdb.statement.CountContainersByNameStatement;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static liquibase.servicelocator.PrioritizedService.PRIORITY_DATABASE;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class CosmosHistoryServiceIT extends AbstractCosmosWithConnectionIntegrationTest {

    protected Liquibase liquibase;

    public CosmosHistoryService cosmosHistoryService;
    private CountContainersByNameStatement countContainersByNameStatement;
    protected CosmosExecutor cosmosExecutor;


    @BeforeEach
    protected void setUpEach() {
        super.setUpEach();
        cosmosHistoryService = (CosmosHistoryService) ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(cosmosLiquibaseDatabase);
        cosmosExecutor = (CosmosExecutor) Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor(CosmosExecutor.COSMOS_EXECUTOR_NAME, cosmosLiquibaseDatabase);
        cosmosHistoryService.reset();
        cosmosHistoryService.resetDeploymentId();
        countContainersByNameStatement = new CountContainersByNameStatement(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName());
    }

    @Test
    void testGetPriority() {
        assertThat(cosmosHistoryService.getPriority()).isEqualTo(PRIORITY_DATABASE);
    }

    @Test
    void testSupports() {
        assertThat(cosmosHistoryService.supports(cosmosLiquibaseDatabase)).isTrue();
        assertThat(cosmosHistoryService.supports(new H2Database())).isFalse();

    }

    @Test
    void testGetDatabaseChangeLogTableName() {
        assertThat(cosmosHistoryService.getDatabaseChangeLogTableName()).isEqualTo(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName());
    }

    @Test
    void testCanCreateChangeLogTable() {
        assertThat(cosmosHistoryService.canCreateChangeLogTable()).isTrue();
    }

    @SneakyThrows
    @Test
    void testInit() {
        assertThat(cosmosHistoryService.getRanChangeSetList()).isNull();
        assertThat(cosmosHistoryService.isServiceInitialized()).isFalse();
        assertThat(cosmosHistoryService.getHasDatabaseChangeLogTable()).isNull();
        assertThat(cosmosHistoryService.getExecutor()).isEqualTo(cosmosExecutor);
        assertThat(countContainersByNameStatement.queryForLong(cosmosDatabase)).isEqualTo(0L);
        cosmosHistoryService.init();
        assertThat(cosmosHistoryService.getRanChangeSetList()).isNull();
        assertThat(cosmosHistoryService.isServiceInitialized()).isTrue();
        assertThat(cosmosHistoryService.getHasDatabaseChangeLogTable()).isTrue();
        assertThat(cosmosHistoryService.getExecutor()).isEqualTo(cosmosExecutor);
        assertThat(countContainersByNameStatement.queryForLong(cosmosDatabase)).isEqualTo(1L);
    }

    @SneakyThrows
    @Test
    void testReset() {
        cosmosHistoryService.init();

        cosmosHistoryService.reset();
        assertThat(cosmosHistoryService.getRanChangeSetList()).isNull();
        assertThat(cosmosHistoryService.isServiceInitialized()).isFalse();
        assertThat(cosmosHistoryService.getHasDatabaseChangeLogTable()).isNull();
        assertThat(cosmosHistoryService.getExecutor()).isEqualTo(cosmosExecutor);
        assertThat(countContainersByNameStatement.queryForLong(cosmosDatabase)).isEqualTo(1L);
    }

    @Test
    @Disabled
    void testUpgradeChecksums() {
    //TODO: implement
    }

    @SneakyThrows
    @Test
    void testGetRanChangeSets() {
        cosmosHistoryService.init();
        assertThat(cosmosHistoryService.getRanChangeSetList()).isNull();

        final List<RanChangeSet> ranChangeSetList = cosmosHistoryService.getRanChangeSetList();
        assertThat(ranChangeSetList).isNull();
    }

    @Test
    @Disabled
    void testReplaceChecksum() throws Exception {
    //TODO: implement

    //        initLiquibase();
    //
    //        final ChangeSet changeSet = liquibase.getDatabaseChangeLog().getChangeSet(FILE_PATH, "alex", "1");
    //        assertTrue(cosmosHistoryService.isServiceInitialized());
    //        //Scope.getCurrentScope().getSingleton(ExecutorService.class).setExecutor(database, cosmosExecutor);
    //
    //        cosmosHistoryService.replaceChecksum(changeSet);
    //
    //        assertFalse(cosmosHistoryService.isServiceInitialized());
    }

    @SneakyThrows
    @Test
    void testGetRanChangeSet() {
        cosmosHistoryService.init();

        assertThat(cosmosHistoryService.getRanChangeSetList()).isNull();
        assertThat(cosmosHistoryService.getRanChangeSets()).hasSize(0);
        assertThat(cosmosHistoryService.getRanChangeSetList()).hasSize(0);
    }

    @Test
    @Disabled
    void testSetExecType() {
        //TODO: implement
    }

    @Test
    @Disabled
    void testRemoveFromHistory() {
        //TODO: implement

    //        initLiquibase();
    //
    //        final ChangeSet changeSet = liquibase.getDatabaseChangeLog().getChangeSet(FILE_PATH, "alex", "1");
    //
    //        assertThat(cosmosHistoryService.getRanChangeSets()).hasSize(1);
    //
    //        cosmosHistoryService.removeFromHistory(changeSet);
    //
    //        assertTrue(cosmosHistoryService.getRanChangeSets().isEmpty());
    //        assertTrue(cosmosHistoryService.isServiceInitialized());
    }

    @SneakyThrows
    @Test
    void testGetNextSequenceValue() {
        cosmosHistoryService.init();
        assertThat(cosmosHistoryService.getLastChangeSetSequenceValue()).isNull();
        final int nextSequenceValue1 = cosmosHistoryService.getNextSequenceValue();
        assertThat(nextSequenceValue1).isEqualTo(1);
        assertThat(cosmosHistoryService.getLastChangeSetSequenceValue()).isEqualTo(1);

    }

    @Test
    @Disabled
    void testTag() {
        //TODO: implement
    }

    @Test
    @Disabled
    void testTagExists() {
        //TODO: implement
    }

    @Test
    @Disabled
    void testClearAllCheckSums() {
        //TODO: implement
    }

    @SneakyThrows
    @Test
    void testDestroy() {
        cosmosHistoryService.init();
        
        cosmosHistoryService.destroy();
        assertThat(cosmosHistoryService.getRanChangeSetList()).isNull();
        assertThat(cosmosHistoryService.isServiceInitialized()).isFalse();
        assertThat(cosmosHistoryService.getHasDatabaseChangeLogTable()).isNull();
        assertThat(cosmosHistoryService.getExecutor()).isEqualTo(cosmosExecutor);
        assertThat(countContainersByNameStatement.queryForLong(cosmosDatabase)).isEqualTo(0L);
        
    }
}
