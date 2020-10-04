package liquibase.ext.cosmosdb.lockservice;

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

import liquibase.Scope;
import liquibase.configuration.GlobalConfiguration;
import liquibase.configuration.LiquibaseConfiguration;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.LockException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.executor.ExecutorService;
import liquibase.ext.cosmosdb.database.CosmosConnection;
import liquibase.ext.cosmosdb.database.CosmosLiquibaseDatabase;
import liquibase.ext.cosmosdb.executor.CosmosExecutor;
import liquibase.ext.cosmosdb.statement.CountContainersByNameStatement;
import liquibase.ext.cosmosdb.statement.DeleteContainerStatement;
import liquibase.lockservice.DatabaseChangeLogLock;
import liquibase.lockservice.LockService;
import liquibase.logging.Logger;
import lombok.Getter;

import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.isNull;

public class CosmosLockService implements LockService {

    private final Logger log = Scope.getCurrentScope().getLog(getClass());

    @Getter
    private CosmosLiquibaseDatabase database;

    private boolean hasChangeLogLock;

    @Getter
    private Long changeLogLockPollRate;

    private Long changeLogLockRecheckTime;

    @Getter
    private Boolean hasDatabaseChangeLogLockTable;

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public boolean supports(Database database) {
        return CosmosLiquibaseDatabase.COSMOSDB_PRODUCT_NAME.equals(database.getDatabaseProductName());
    }

    @Override
    public void setDatabase(Database database) {
        this.database = (CosmosLiquibaseDatabase) database;
    }

    public CosmosExecutor getExecutor() {
        return (CosmosExecutor) Scope.getCurrentScope().getSingleton(ExecutorService.class).getExecutor(CosmosExecutor.COSMOS_EXECUTOR_NAME, getDatabase());
    }

    @Override
    public void init() throws DatabaseException {

        final CosmosExecutor executor = getExecutor();

        if (!hasDatabaseChangeLogLockTable()) {

            log.info("Create Database Lock Container: "
                    + ((CosmosConnection)getDatabase().getConnection()).getCosmosDatabase().getId() + "." + getDatabaseChangeLogLockTableName());
            final CreateChangeLogLockContainerStatement createChangeLogLockContainerStatement =
                    new CreateChangeLogLockContainerStatement(getDatabaseChangeLogLockTableName());

            executor.execute(createChangeLogLockContainerStatement);
            database.commit();
            log.fine("Created database lock Container: " + createChangeLogLockContainerStatement.toJs());
            this.hasDatabaseChangeLogLockTable = true;
        }
    }

    @Override
    public boolean hasChangeLogLock() {
        return hasChangeLogLock;
    }

    @Override
    public void waitForLock() throws LockException {

        boolean locked = false;

        long timeToGiveUp = new Date().getTime() + (getChangeLogLockWaitTime() * 1000 * 60);
        while (!locked && (new Date().getTime() < timeToGiveUp)) {
            locked = acquireLock();
            if (!locked) {
                log.info("Waiting for changelog lock....");
                try {
                    //noinspection BusyWait
                    Thread.sleep(getChangeLogLockRecheckTime() * 1000);
                } catch (InterruptedException e) {
                    // Restore thread interrupt status
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (!locked) {
            DatabaseChangeLogLock[] locks = listLocks();
            String lockedBy;
            if (locks.length > 0) {
                DatabaseChangeLogLock lock = locks[0];
                lockedBy = lock.getLockedBy() + " since " +
                        DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT)
                                .format(lock.getLockGranted());
            } else {
                lockedBy = "UNKNOWN";
            }
            throw new LockException("Could not acquire change log lock.  Currently locked by " + lockedBy);
        }
    }

    @Override
    public boolean acquireLock() throws LockException {
        if (hasChangeLogLock) {
            return true;
        }

        try {
            database.rollback();
            this.init();

            final SelectChangeLogLockStatement selectChangeLogLockStatement =
                    new SelectChangeLogLockStatement(getDatabaseChangeLogLockTableName());

            final Optional<CosmosChangeLogLock> lock =
                    Optional.ofNullable(getExecutor().queryForObject(selectChangeLogLockStatement, CosmosChangeLogLock.class));

            if (lock.isPresent() && lock.get().getLocked()) {
                return false;
            } else {
                log.info("Lock Database");

                final int rowsUpdated = getExecutor().update(new ReplaceLockChangeLogStatement(getDatabaseChangeLogLockTableName(), true));

                if (rowsUpdated > 1) {
                    throw new LockException("Did not update change log lock correctly");
                }
                if (rowsUpdated == 0) {
                    // another node was faster
                    return false;
                }

                database.commit();
                log.info("Successfully Acquired Change Log Lock");

                this.hasChangeLogLock = true;
                this.database.setCanCacheLiquibaseTableInfo(true);

                return true;
            }
        } catch (final Exception e) {
            throw new LockException(e);
        } finally {
            try {
                database.rollback();
            } catch (DatabaseException e) {
                log.severe("Error on acquire change log lock Rollback.", e);
            }
        }
    }

    @Override
    public void releaseLock() throws LockException {

        try {
            if (this.hasDatabaseChangeLogLockTable()) {

                log.info("Release Database Lock");

                database.rollback();

                final int rowsUpdated =
                        getExecutor().update(new ReplaceLockChangeLogStatement(getDatabaseChangeLogLockTableName(), false));

                if (rowsUpdated != 1) {
                    throw new LockException("Did not update change log lock correctly.\n\n" +
                                    rowsUpdated +
                                    " rows were updated instead of the expected 1 row " +
                                    " there are more than one rows in the table"
                    );
                }
                database.commit();
            }
        } catch (Exception e) {
            throw new LockException(e);
        } finally {
            try {
                this.hasChangeLogLock = false;
                database.setCanCacheLiquibaseTableInfo(false);
                log.info("Successfully released change log lock");
                database.rollback();
            } catch (DatabaseException e) {
                log.severe("Error on released change log lock Rollback.", e);
            }
        }
    }

    @Override
    public DatabaseChangeLogLock[] listLocks() throws LockException {
        try {
            if (!this.hasDatabaseChangeLogLockTable()) {
                return new DatabaseChangeLogLock[0];
            }
            final List<Object> rows =
                    getExecutor().queryForList(new SelectChangeLogLocksStatement(getDatabaseChangeLogLockTableName()), CosmosChangeLogLock.class);
            return rows.stream().map(CosmosChangeLogLock.class::cast).toArray(DatabaseChangeLogLock[]::new);
        } catch (final Exception e) {
            throw new LockException(e);
        }
    }

    @Override
    public void forceReleaseLock() throws LockException, DatabaseException {
        this.init();
        releaseLock();
    }

    @Override
    public void reset() {
        hasChangeLogLock = false;
        hasDatabaseChangeLogLockTable = null;
    }

    @Override
    public void destroy() {
        try {
            final CosmosExecutor executor = getExecutor();

            log.info("Dropping Container Database Change Log Lock: " + getDatabaseChangeLogLockTableName());
                executor.execute(
                        new DeleteContainerStatement(getDatabaseChangeLogLockTableName()));
                hasDatabaseChangeLogLockTable = null;
            database.commit();
            reset();
        } catch (final DatabaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }
    }

    public String getDatabaseChangeLogLockTableName() {
        return database.getDatabaseChangeLogLockTableName();
    }

    private Long getChangeLogLockRecheckTime() {
        if (changeLogLockRecheckTime != null) {
            return changeLogLockRecheckTime;
        }
        return LiquibaseConfiguration
                .getInstance()
                .getConfiguration(GlobalConfiguration.class)
                .getDatabaseChangeLogLockPollRate();
    }

    @Override
    public void setChangeLogLockRecheckTime(long changeLogLockRecheckTime) {
        this.changeLogLockRecheckTime = changeLogLockRecheckTime;
    }

    private Long getChangeLogLockWaitTime() {
        if (changeLogLockPollRate != null) {
            return changeLogLockPollRate;
        }
        return LiquibaseConfiguration
                .getInstance()
                .getConfiguration(GlobalConfiguration.class)
                .getDatabaseChangeLogLockWaitTime();
    }

    @Override
    public void setChangeLogLockWaitTime(long changeLogLockWaitTime) {
        this.changeLogLockPollRate = changeLogLockWaitTime;
    }

    private boolean hasDatabaseChangeLogLockTable() throws DatabaseException {
        if (isNull(this.hasDatabaseChangeLogLockTable)) {
            try {
                final CosmosExecutor executor = getExecutor();
                this.hasDatabaseChangeLogLockTable =
                        executor.queryForLong(new CountContainersByNameStatement(getDatabase().getDatabaseChangeLogLockTableName())) == 1L;
            } catch (final Exception e) {
                throw new DatabaseException(e);
            }
        }
        return this.hasDatabaseChangeLogLockTable;
    }

}
