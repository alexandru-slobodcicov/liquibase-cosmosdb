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

import liquibase.lockservice.DatabaseChangeLogLock;
import lombok.*;
import lombok.experimental.FieldNameConstants;

import java.util.Date;

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

    public CosmosChangeLogLock() {
        this(1, new Date(), "default", false);
    }

    public CosmosChangeLogLock(final int id, final Date lockGranted, final String lockedBy, final Boolean locked) {
        super(id, lockGranted, lockedBy);
        this.id = id;
        this.lockGranted = lockGranted;
        this.lockedBy = lockedBy;
        this.locked = locked;
    }
}
