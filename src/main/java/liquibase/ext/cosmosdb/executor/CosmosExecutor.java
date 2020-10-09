package liquibase.ext.cosmosdb.executor;

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
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.executor.AbstractExecutor;
import liquibase.ext.cosmosdb.database.CosmosConnection;
import liquibase.ext.cosmosdb.statement.*;
import liquibase.logging.LogService;
import liquibase.logging.Logger;
import liquibase.servicelocator.LiquibaseService;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

@LiquibaseService
public class CosmosExecutor extends AbstractExecutor {

    public static final String COSMOS_EXECUTOR_NAME = "jdbc";
    private final Logger log = LogService.getLog(getClass());

    public CosmosExecutor() {
        super();
    }

    @Getter
    @Setter
    public CosmosDatabase cosmosDatabase;

    @Override
    public void setDatabase(Database database) {
        super.setDatabase(database);
        cosmosDatabase = ((CosmosConnection) this.database.getConnection()).getCosmosDatabase();
    }

    @Override
    public <T> T queryForObject(final SqlStatement sql, final Class<T> requiredType) throws DatabaseException {
        return queryForObject(sql, requiredType, emptyList());
    }

    @Override
    public <T> T queryForObject(final SqlStatement sql, final Class<T> requiredType, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof NoSqlQueryForObjectStatement) {
            try {
                return ((NoSqlQueryForObjectStatement) sql).queryForObject(cosmosDatabase, requiredType);
            } catch (final Exception e) {
                throw new DatabaseException("Could not query for object", e);
            }
        }
        throw new IllegalArgumentException();
    }

    @Override
    public long queryForLong(final SqlStatement sql) throws DatabaseException {
        return queryForLong(sql, emptyList());
    }

    @Override
    public long queryForLong(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof NoSqlQueryForLongStatement) {
            try {
                return ((NoSqlQueryForLongStatement) sql).queryForLong(cosmosDatabase);
            } catch (final Exception e) {
                throw new DatabaseException("Could not query for long", e);
            }
        }
        throw new IllegalArgumentException();
    }

    @Override
    public int queryForInt(final SqlStatement sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int queryForInt(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Object> queryForList(final SqlStatement sql, final Class elementType) throws DatabaseException {
        return queryForList(sql, elementType, emptyList());
    }

    @Override
    public List<Object> queryForList(final SqlStatement sql, final Class elementType, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof NoSqlQueryForListStatement) {
            try {
                return ((NoSqlQueryForListStatement) sql).queryForList(cosmosDatabase);
            } catch (final Exception e) {
                throw new DatabaseException("Could not query for list", e);
            }
        }
        throw new IllegalArgumentException();
    }

    @Override
    public List<Map<String, ?>> queryForList(final SqlStatement sql) {
        return queryForList(sql, emptyList());
    }

    @Override
    public List<Map<String, ?>> queryForList(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void execute(final SqlStatement sql) throws DatabaseException {
        this.execute(sql, emptyList());
    }

    @Override
    public void execute(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof NoSqlExecuteStatement) {
            try {
                ((NoSqlExecuteStatement) sql).execute(cosmosDatabase);
            } catch (final Exception e) {
                throw new DatabaseException("Could not execute", e);
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public int update(final SqlStatement sql) throws DatabaseException {
        return update(sql, emptyList());
    }

    @Override
    public int update(final SqlStatement sql, final List<SqlVisitor> sqlVisitors) throws DatabaseException {
        if (sql instanceof NoSqlUpdateStatement) {
            try {
                return ((NoSqlUpdateStatement) sql).update(cosmosDatabase);
            } catch (final Exception e) {
                throw new DatabaseException("Could not execute", e);
            }
        } else {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public void comment(final String message) {
        log.info(message);
    }

    @Override
    public boolean updatesDatabase() {
        return true;
    }
}
