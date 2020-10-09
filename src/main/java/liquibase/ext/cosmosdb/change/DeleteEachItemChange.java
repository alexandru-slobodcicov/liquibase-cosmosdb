package liquibase.ext.cosmosdb.change;

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

import liquibase.change.ChangeMetaData;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.ext.cosmosdb.statement.DeleteEachItemStatement;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@DatabaseChange(name = "deleteEachItem",
        description = "Deletes in a loop each Document in a Query " +
                "https://docs.microsoft.com/en-us/java/api/com.azure.cosmos.cosmoscontainer.deleteitem?view=azure-java-stable\n" +
                "https://docs.microsoft.com/en-us/rest/api/cosmos-db/delete-a-document\n" +
                "https://docs.microsoft.com/en-us/rest/api/cosmos-db/query-documents",
        priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "container")
@NoArgsConstructor
@Getter
@Setter
public class DeleteEachItemChange extends AbstractCosmosChange {

    private String containerName;
    private String query;

    @Override
    public String getConfirmationMessage() {
        return "Items deleted in container: " + containerName;
    }

    @Override
    public SqlStatement[] generateStatements(final Database database) {

        final DeleteEachItemStatement deleteEachItemStatement
                = new DeleteEachItemStatement(containerName, query);

        return new SqlStatement[]{
                deleteEachItemStatement
        };
    }
}
