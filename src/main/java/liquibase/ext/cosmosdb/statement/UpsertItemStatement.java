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
import com.azure.cosmos.implementation.Document;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static liquibase.ext.cosmosdb.statement.JsonUtils.orEmptyDocument;

@Getter
@EqualsAndHashCode(callSuper = true)
public class UpsertItemStatement extends CreateItemStatement {

    public static final String COMMAND_NAME = "upsertItem";

    public UpsertItemStatement(final String containerName, final String jsonDocument) {
        this(containerName, orEmptyDocument(jsonDocument));
    }

    public UpsertItemStatement(final String containerName, final Document document) {
        super(containerName, document);
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public void execute(final CosmosDatabase cosmosDatabase) {

        final CosmosContainer cosmosContainer = cosmosDatabase.getContainer(getContainerName());
        cosmosContainer.upsertItem(getDocument());
    }

    @Override
    public String toString() {
        return this.toJs();
    }
}
