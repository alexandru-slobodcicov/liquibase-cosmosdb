package liquibase.ext.cosmosdb;

/*-
 * #%L
 * Liquibase MongoDB Extension
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

import com.azure.cosmos.models.CosmosContainerProperties;
import liquibase.Liquibase;
import liquibase.ext.cosmosdb.changelog.CosmosRanChangeSet;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static liquibase.ext.cosmosdb.persistence.AbstractRepository.DEFAULT_PARTITION_KEY;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.assertj.core.api.Assertions.assertThat;

class CosmosLiquibaseIT extends AbstractCosmosWithConnectionIntegrationTest {

    @SneakyThrows
    @Test
    @SuppressWarnings("unchecked")
    void testUpdate() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.generic.test.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);
        liquibase.update(EMPTY);
        assertThat(cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogLockTableName()).read()).isNotNull();
        assertThat(cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName()).read()).isNotNull();
        assertThat(cosmosDatabase.readAllContainers().stream().count()).isEqualTo(3);

        final List<Map<String, Object>> ranChangeSets = cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName()).readAllItems(DEFAULT_PARTITION_KEY, Map.class)
                .stream().map(i -> (Map<String, Object>) i).collect(Collectors.toList());
        assertThat(ranChangeSets).hasSize(2);
        assertThat(ranChangeSets.get(0))
                .hasFieldOrPropertyWithValue(CosmosRanChangeSet.Fields.ORDER_EXECUTED, 1);
        assertThat(ranChangeSets.get(1))
                .hasFieldOrPropertyWithValue(CosmosRanChangeSet.Fields.ORDER_EXECUTED, 2);
    }

    @SneakyThrows
    @Test
    void testUpdateCreateContainer() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.create-container.test.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);
        liquibase.update(EMPTY);
        assertThat(cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogLockTableName()).read()).isNotNull();
        assertThat(cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName()).read()).isNotNull();

        List<CosmosContainerProperties> containerProperties = cosmosDatabase.readAllContainers().stream().collect(Collectors.toList());

        final CosmosContainerProperties minimal = containerProperties.stream().filter(c -> c.getId().equals("minimal")).findFirst().orElse(null);
        assertThat(minimal).isNotNull();

        final CosmosContainerProperties skipExisting = containerProperties.stream().filter(c -> c.getId().equals("skipExisting")).findFirst().orElse(null);
        assertThat(skipExisting).isNotNull();

        final CosmosContainerProperties maximal = containerProperties.stream().filter(c -> c.getId().equals("maximal")).findFirst().orElse(null);
        assertThat(maximal).isNotNull();

        assertThat(containerProperties).hasSize(5);
    }

    @SneakyThrows
    @Test
    void testUpdateDeleteContainer() {

        assertThat(cosmosDatabase.readAllContainers().stream().count()).isEqualTo(0L);

        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.delete-container.test.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);
        liquibase.update(EMPTY);
        assertThat(cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogLockTableName()).read()).isNotNull();
        assertThat(cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName()).read()).isNotNull();
        assertThat(cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName())
                .readAllItems(DEFAULT_PARTITION_KEY, Map.class).stream().count()).isEqualTo(2L);

        assertThat(cosmosDatabase.readAllContainers().stream().map(CosmosContainerProperties::getId).collect(Collectors.toList()))
                .hasSize(3)
                .contains("container4")
                .doesNotContain("container1", "container2", "container3", "container5");
    }

    @SneakyThrows
    @Test
    void testUpdateIncremental() {

        // First increment
        Liquibase liquibase = new Liquibase("liquibase/ext/changelog.incremental-1.main.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName())).findFirst())
                .isNotPresent();
        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals(cosmosLiquibaseDatabase.getDatabaseChangeLogLockTableName())).findFirst())
                .isNotPresent();

        liquibase.update(EMPTY);

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName())).findFirst())
                .isPresent();

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals(cosmosLiquibaseDatabase.getDatabaseChangeLogLockTableName())).findFirst())
                .isPresent();

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals("container1")).findFirst())
                .isPresent();

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals("container2")).findFirst())
                .isNotPresent();

        assertThat(cosmosDatabase.readAllContainers().stream().count()).isEqualTo(3);

        // Second increment
        liquibase = new Liquibase("liquibase/ext/changelog.incremental-2.main.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName())).findFirst())
                .isPresent();
        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals(cosmosLiquibaseDatabase.getDatabaseChangeLogLockTableName())).findFirst())
                .isPresent();

        liquibase.update(EMPTY);

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName())).findFirst())
                .isPresent();

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals(cosmosLiquibaseDatabase.getDatabaseChangeLogLockTableName())).findFirst())
                .isPresent();

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals("container1")).findFirst())
                .isPresent();

        assertThat(cosmosDatabase.readAllContainers().stream()
                .filter(c -> c.getId().equals("container2")).findFirst())
                .isPresent();

        assertThat(cosmosDatabase.readAllContainers().stream().count()).isEqualTo(4);


    }

    @SneakyThrows
    @Test
    void testUpdateCreateItem() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.create-item.test.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);
        liquibase.update(EMPTY);

        Map<?, ?> document = cosmosDatabase.getContainer("container1").readAllItems(DEFAULT_PARTITION_KEY, Map.class)
                .stream().filter(i -> ((Map<?, ?>) i).get("id").equals("1")).findFirst().orElse(null);

        assertThat(document).isNotNull();
        assertThat(document.get("specialField($)!")).isEqualTo("Special char in field name");
        assertThat(document.get("integerField")).isEqualTo(100);
        assertThat(document.get("numberField")).isEqualTo(-12.2);
        assertThat(document.get("nullField")).isNull();
        assertThat((ArrayList<?>) document.get("arrayField")).hasSize(3);
        assertThat((ArrayList<?>) document.get("emptyArray")).isEmpty();
        assertThat((Map<?, ?>) document.get("nestedObject")).hasSize(2);
        assertThat(((Map<?, ?>) document.get("nestedObject")).get("nestedField")).isEqualTo("nestedValue");
        assertThat((ArrayList<?>) ((Map<?, ?>) document.get("nestedObject")).get("nestedArrayField")).hasSize(3);
        assertThat((Map<?, ?>) document.get("nestedEmptyObject")).isEmpty();
        assertThat(document.get("partition")).isEqualTo("default");
    }

    @SneakyThrows
    @Test
    void testUpdateUpsertItem() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.upsert-item.test.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);
        liquibase.update(EMPTY);

        // createContainer, createItem, upsertItem = 3 rows in log
        assertThat(cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName())
                .readAllItems(DEFAULT_PARTITION_KEY, Map.class).stream().count()).isEqualTo(3L);

        Map<?, ?> document = cosmosDatabase.getContainer("container1").readAllItems(DEFAULT_PARTITION_KEY, Map.class)
                .stream().filter(i -> ((Map<?, ?>) i).get("id").equals("1")).findFirst().orElse(null);

        assertThat(document).isNotNull();
        assertThat(document.get("oldField")).isNull();
        assertThat(document.get("changeValueField")).isEqualTo("New Value");
        assertThat(document.get("sameValueField")).isEqualTo("Remains Same");
        assertThat(document.get("newField")).isEqualTo("Will be Added");
        assertThat(document.get("partition")).isEqualTo("default");
    }


    @SneakyThrows
    @Test
    @SuppressWarnings("unchecked")
    void testUpdateUpdateEachItem() {
        final Liquibase liquibase = new Liquibase("liquibase/ext/changelog.update-each-item.test.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);
        liquibase.update(EMPTY);

        // createContainer, createItem, upsertItem = 3 rows in log
        assertThat(cosmosDatabase.getContainer(cosmosLiquibaseDatabase.getDatabaseChangeLogTableName())
                .readAllItems(DEFAULT_PARTITION_KEY, Map.class).stream().count()).isEqualTo(4L);

        Map<String, ?> document = cosmosDatabase.getContainer("container1").readAllItems(DEFAULT_PARTITION_KEY, Map.class)
                .stream().filter(i -> ((Map<String, ?>) i).get("id").equals("1")).findFirst().orElse(null);

        assertThat(document).isNotNull()
                .hasFieldOrPropertyWithValue("id", "1")
                .hasFieldOrPropertyWithValue("changedField", "Value Changed")
                .hasFieldOrPropertyWithValue("remainedField", "Remains Same1")
                .hasFieldOrPropertyWithValue("onlyIn1Field", "Remains Only1")
                .doesNotContainKey("onlyIn2Field")
                .doesNotContainKey("onlyIn3Field")
                .hasFieldOrPropertyWithValue("addedField", "Added Value")
                .hasFieldOrPropertyWithValue("partition", "default");

        document = cosmosDatabase.getContainer("container1").readAllItems(DEFAULT_PARTITION_KEY, Map.class)
                .stream().filter(i -> ((Map<String, ?>) i).get("id").equals("2")).findFirst().orElse(null);

        assertThat(document).isNotNull()
                .hasFieldOrPropertyWithValue("id", "2")
                .hasFieldOrPropertyWithValue("changedField", "Value Changed")
                .hasFieldOrPropertyWithValue("remainedField", "Remains Same2")
                .hasFieldOrPropertyWithValue("onlyIn2Field", "Remains Only2")
                .doesNotContainKey("onlyIn1Field")
                .doesNotContainKey("onlyIn3Field")
                .hasFieldOrPropertyWithValue("addedField", "Added Value")
                .hasFieldOrPropertyWithValue("partition", "default");

        document = cosmosDatabase.getContainer("container1").readAllItems(DEFAULT_PARTITION_KEY, Map.class)
                .stream().filter(i -> ((Map<String, ?>) i).get("id").equals("3")).findFirst().orElse(null);

        assertThat(document).isNotNull()
                .hasFieldOrPropertyWithValue("id", "3")
                .hasFieldOrPropertyWithValue("changedField", "Value Unchanged")
                .hasFieldOrPropertyWithValue("remainedField", "Remains Same3")
                .hasFieldOrPropertyWithValue("onlyIn3Field", "Remains Only3")
                .doesNotContainKey("onlyIn1Field")
                .doesNotContainKey("onlyIn2Field")
                .doesNotContainKey("addedField")
                .hasFieldOrPropertyWithValue("partition", "default");
    }

    @Test
    void testClearChecksums() {
        //Liquibase liquibase = new Liquibase("liquibase/ext/changelog.create-item.test.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);
        //TODO: liquibase.clearCheckSums();
    }

    @Test
    void testClearScopes() {
        //Liquibase liquibase = new Liquibase("liquibase/ext/changelog.create-item.test.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);
        //TODO: scopes;
    }

    @SneakyThrows
    @Test
    void testDropAll() {
        Liquibase liquibase = new Liquibase("liquibase/ext/changelog.generic.test.xml", new ClassLoaderResourceAccessor(), cosmosLiquibaseDatabase);
        liquibase.dropAll();
        assertThat(cosmosDatabase.readAllContainers().stream().count()).isEqualTo(0);

    }

}
