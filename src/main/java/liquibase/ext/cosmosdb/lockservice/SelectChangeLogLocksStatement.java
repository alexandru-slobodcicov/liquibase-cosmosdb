package liquibase.ext.cosmosdb.lockservice;

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

import com.azure.cosmos.CosmosDatabase;
import liquibase.ext.cosmosdb.statement.AbstractNoSqlContainerStatement;
import liquibase.ext.cosmosdb.statement.NoSqlQueryForListStatement;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
public class SelectChangeLogLocksStatement extends AbstractNoSqlContainerStatement implements NoSqlQueryForListStatement {

    public static final String COMMAND_NAME = "selectLocks";

    public SelectChangeLogLocksStatement(final String containerName) {
        super(containerName);
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    public List<CosmosChangeLogLock> readAll(final CosmosDatabase cosmosDatabase) {
        final ChangeLogLockRepository repository = new ChangeLogLockRepository(cosmosDatabase, getContainerName());
        return repository.getAll().stream().filter(CosmosChangeLogLock::getLocked).collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public <T> List<T> queryForList(final CosmosDatabase cosmosDatabase) {
        return (List<T>) readAll(cosmosDatabase);
    }
}
