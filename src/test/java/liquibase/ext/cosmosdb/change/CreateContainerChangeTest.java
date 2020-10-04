package liquibase.ext.cosmosdb.change;

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

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CreateContainerChangeTest extends AbstractCosmosChangeTest {

    @Test
    void getConfirmationMessage() {
        assertThat(new CreateContainerChange().getConfirmationMessage())
            .isNotNull();
    }

    @Test
    @SneakyThrows
    void testGenerateStatements() {

        //TODO: fix

//        final List<ChangeSet> changeSets = getChangesets("liquibase/ext/changelog.create-container.test.xml", database);
//
//        assertThat(changeSets)
//            .isNotNull()
//            .hasSize(1);
//        assertThat(changeSets.get(0).getChanges())
//            .hasSize(3)
//            .hasOnlyElementsOfType(CreateContainerChange.class);
//
//        final CreateContainerChange ch1 = (CreateContainerChange) changeSets.get(0).getChanges().get(0);
//        assertThat(ch1.getContainerName()).isEqualTo("createCollectionWithValidatorAndOptionsTest");
//        assertThat(ch1.getPartitionKeyPath()).isNotBlank();
//        final SqlStatement[] sqlStatement1 = ch1.generateStatements(database);
//        assertThat(sqlStatement1)
//            .hasSize(1);
//        assertThat(((CreateContainerStatement) sqlStatement1[0]))
//            .hasNoNullFieldsOrProperties()
//            .hasFieldOrPropertyWithValue("collectionName", "createCollectionWithValidatorAndOptionsTest");
//
//        final CreateContainerChange ch2 = (CreateContainerChange) changeSets.get(0).getChanges().get(1);
//        assertThat(ch2.getContainerName()).isEqualTo("createCollectionWithEmptyValidatorTest");
//        assertThat(ch2.getPartitionKeyPath()).isBlank();
//        final SqlStatement[] sqlStatement2 = ch2.generateStatements(database);
//        assertThat(sqlStatement2)
//            .hasSize(1);
//        assertThat(((CreateContainerStatement) sqlStatement2[0]))
//            .hasNoNullFieldsOrPropertiesExcept("options")
//            .hasFieldOrPropertyWithValue("collectionName", "createCollectionWithEmptyValidatorTest");
//
//        final CreateContainerChange ch3 = (CreateContainerChange) changeSets.get(0).getChanges().get(2);
//        assertThat(ch3.getContainerName()).isEqualTo("createCollectionWithNoValidator");
//        assertThat(ch3.getPartitionKeyPath()).isBlank();
//        final SqlStatement[] sqlStatement3 = ch3.generateStatements(database);
//        assertThat(sqlStatement3)
//            .hasSize(1);
//        assertThat(((CreateContainerStatement) sqlStatement3[0]))
//            .hasNoNullFieldsOrPropertiesExcept("options")
//            .hasFieldOrPropertyWithValue("collectionName", "createCollectionWithNoValidator");
    }
}
