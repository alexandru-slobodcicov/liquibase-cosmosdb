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

import com.azure.cosmos.CosmosDatabase;
import liquibase.changelog.RanChangeSet;
import liquibase.ext.cosmosdb.statement.AbstractNoSqlContainerStatement;
import liquibase.ext.cosmosdb.statement.NoSqlQueryForListStatement;
import lombok.Getter;
import lombok.Setter;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
public class SelectChangeLogRanChangeSetsStatement extends AbstractNoSqlContainerStatement implements NoSqlQueryForListStatement {

    public static final String COMMAND_NAME = "selectRanChangeSets";

    public SelectChangeLogRanChangeSetsStatement(final String containerName) {
        super(containerName);
    }

    @Override
    public String getCommandName() {
        return COMMAND_NAME;
    }

    public List<CosmosRanChangeSet> readAll(final CosmosDatabase cosmosDatabase) {
        final ChangeSetRepository repository = new ChangeSetRepository(cosmosDatabase, getContainerName());
        return repository.getAll().stream()
                .sorted(Comparator.comparing(RanChangeSet::getDateExecuted)).collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> queryForList(final CosmosDatabase cosmosDatabase) {
        return (List<T>) readAll(cosmosDatabase);
    }
}
