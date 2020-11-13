package liquibase.ext.cosmosdb.changelog;

import liquibase.ContextExpression;
import liquibase.Labels;
import liquibase.change.CheckSum;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.RanChangeSet;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.Collection;
import java.util.Date;

@EqualsAndHashCode(callSuper = true)
public class CosmosRanChangeSet extends RanChangeSet {

    public static class Fields {
        public static final String FILE_NAME = "fileName";
        public static final String CHANGE_SET_ID = "changeSetId";
        public static final String AUTHOR = "author";
        public static final String LAST_CHECK_SUM = "md5sum";
        public static final String DATE_EXECUTED = "dateExecuted";
        public static final String TAG = "tag";
        public static final String EXEC_TYPE = "execType";
        public static final String DESCRIPTION = "description";
        public static final String COMMENTS = "comments";
        public static final String CONTEXT_EXPRESSION = "contexts";
        public static final String LABELS = "labels";
        public static final String DEPLOYMENT_ID = "deploymentId";
        public static final String ORDER_EXECUTED = "orderExecuted";
        public static final String LIQUIBASE = "liquibase";
    }

    @Getter
    @Setter
    private String uuid;

    @Getter
    @Setter
    private Collection<ContextExpression> inheritableContexts;

    @Getter
    @Setter
    private String liquibase;

    public CosmosRanChangeSet(final String uuid, final String changeLog, final String id, final String author, final CheckSum lastCheckSum, final Date dateExecuted
            , final String tag, final ChangeSet.ExecType execType, final String description, final String comments, final ContextExpression contextExpression, final Collection<ContextExpression> inheritableContexts
            , final Labels labels, final String deploymentId, final Integer orderExecuted, final String liquibase) {
        super(changeLog, id, author, lastCheckSum, dateExecuted, tag, execType, description, comments, contextExpression, labels, deploymentId);
        super.setOrderExecuted(orderExecuted);
        this.uuid = uuid;
        this.inheritableContexts = inheritableContexts;
        this.liquibase = liquibase;
    }

    public CosmosRanChangeSet(final ChangeSet changeSet, final ChangeSet.ExecType execType, final ContextExpression contextExpression, final Labels labels) {
        super(changeSet, execType, contextExpression, labels);
    }
}
