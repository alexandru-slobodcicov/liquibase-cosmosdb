package liquibase.ext.cosmosdb.statement;

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

import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.implementation.Document;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlQuerySpec;
import liquibase.ext.cosmosdb.database.CosmosLiquibaseDatabase;
import liquibase.nosql.statement.NoSqlExecuteStatement;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static liquibase.ext.cosmosdb.statement.JsonUtils.COSMOS_ID_FIELD;
import static liquibase.ext.cosmosdb.statement.JsonUtils.extractPartitionKeyByPath;
import static liquibase.ext.cosmosdb.statement.JsonUtils.extractPartitionKeyPath;
import static liquibase.ext.cosmosdb.statement.JsonUtils.orEmptySqlQuerySpec;

public class DeleteEachItemStatement extends AbstractCosmosContainerStatement
        implements NoSqlExecuteStatement<CosmosLiquibaseDatabase> {

    public static final String COMMAND_NAME = "deleteEachItem";

    private final SqlQuerySpec query;

    public DeleteEachItemStatement(final String containerId, final String jsonQuery) {
        super(containerId);
        this.query = orEmptySqlQuerySpec(jsonQuery);
    }

    public DeleteEachItemStatement(final String containerId, final SqlQuerySpec query) {
        super(containerId);
        this.query = query;
    }

    public DeleteEachItemStatement() {
        this(null, (String) null);
    }

    @Override
    public void execute(final CosmosLiquibaseDatabase database) {
        final CosmosContainer cosmosContainer = database.getCosmosDatabase().getContainer(containerId);
        final String partitionKeyPath = extractPartitionKeyPath(cosmosContainer);

        final List<Document> documents = cosmosContainer
                .queryItems(query, null, Map.class).stream().map(JsonUtils::fromMap)
                .map(Document.class::cast)
                .collect(Collectors.toList());

        documents.forEach(document -> {
            final PartitionKey partitionKey = extractPartitionKeyByPath(document, partitionKeyPath);
            cosmosContainer.deleteItem((String) document.get(COSMOS_ID_FIELD), partitionKey, null);
        });
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }
}
