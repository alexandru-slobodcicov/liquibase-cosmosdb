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
import liquibase.ext.cosmosdb.database.CosmosLiquibaseDatabase;
import liquibase.nosql.statement.NoSqlExecuteStatement;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static liquibase.ext.cosmosdb.statement.JsonUtils.orEmptyDocument;

@Getter
@EqualsAndHashCode(callSuper = true)
public class CreateItemStatement extends AbstractCosmosContainerStatement
        implements NoSqlExecuteStatement<CosmosLiquibaseDatabase> {

    public static final String COMMAND_NAME = "createItem";

    private final Document document;

    public CreateItemStatement() {
        this(null, (String) null);
    }

    public CreateItemStatement(final String containerId, final String jsonDocument) {
        this(containerId, orEmptyDocument(jsonDocument));
    }

    public CreateItemStatement(final String containerId, final Document document) {
        super(containerId);
        this.document = document;
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    @Override
    public String toJs() {
        return
                "db." +
                        containerId +
                        "." +
                        getCommandName() +
                        "(" +
                        document.toJson() +
                        ");";
    }

    @Override
    public void execute(final CosmosLiquibaseDatabase database) {

        final CosmosContainer cosmosContainer = database.getCosmosDatabase().getContainer(containerId);
        cosmosContainer.createItem(document);
    }

}
