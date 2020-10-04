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

import liquibase.lockservice.DatabaseChangeLogLock;
import lombok.*;
import lombok.experimental.FieldNameConstants;

import java.util.Date;

import static liquibase.ext.cosmosdb.persistence.AbstractItemToDocumentConverter.DEFAULT_PARTITION_KEY_VALUE;

@Getter
@Setter
@ToString
@FieldNameConstants
@Builder
@EqualsAndHashCode(callSuper = false)
public class CosmosChangeLogLock extends DatabaseChangeLogLock{

    private int id;
    private Date lockGranted;
    private String lockedBy;
    private Boolean locked;
    private String partition;

    public CosmosChangeLogLock() {
        this(1, new Date(), "default", false);
    }

    public CosmosChangeLogLock(int id, Date lockGranted, String lockedBy, Boolean locked) {
        this(id, lockGranted, lockedBy, locked, DEFAULT_PARTITION_KEY_VALUE);
    }

    public CosmosChangeLogLock(final int id, final Date lockGranted, final String lockedBy, final Boolean locked, final String partition) {
        super(id, lockGranted, lockedBy);
        this.id = id;
        this.lockGranted = lockGranted;
        this.lockedBy = lockedBy;
        this.locked = locked;
        this.partition = partition;
    }
}
