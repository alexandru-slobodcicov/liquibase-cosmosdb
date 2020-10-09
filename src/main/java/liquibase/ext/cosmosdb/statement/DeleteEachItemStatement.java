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
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.SqlQuerySpec;

import java.util.Map;

import static liquibase.ext.cosmosdb.statement.JsonUtils.*;

public class DeleteEachItemStatement extends AbstractNoSqlContainerStatement implements NoSqlExecuteStatement {

    public static final String COMMAND_NAME = "deleteEachItem";

    private final SqlQuerySpec query;

    public DeleteEachItemStatement(final String containerName, final String jsonQuery) {
        super(containerName);
        this.query = orEmptySqlQuerySpec(jsonQuery);
    }

    public DeleteEachItemStatement(final String containerName, final SqlQuerySpec query) {
        super(containerName);
        this.query = query;
    }

    public DeleteEachItemStatement() {
        this(null, (String)null);
    }

    @Override
    public void execute(final CosmosDatabase cosmosDatabase) {
        final CosmosContainer cosmosContainer = cosmosDatabase.getContainer(containerName);

        //TODO: Test with partitions, not clear which one will be deleted
        cosmosContainer.queryItems(query, null, Map.class).stream()
                .forEach(d -> cosmosContainer.deleteItem((String) d.get(COSMOS_ID_FIELD), DEFAULT_PARTITION_KEY_PERSIST,null));
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }
}
