parser grammar ClickHouseParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level statements

queryStmt
    : query queryOutputTail SEMICOLON?        # QueryStmtQuery
    | createTokenStmt SEMICOLON?              # QueryStmtCreateToken
    | queryWithoutOutput SEMICOLON?           # QueryStmtQueryWithoutOutput
    | insertStmt                              # QueryStmtInsert
    | deleteStmt                              # QueryStmtDelete
    | updateStmt                              # QueryStmtUpdate
    ;

// The tail of a query with output. ParserQueryWithOutput reads it in this order:
// INTO OUTFILE with its options, then FORMAT and SETTINGS in any order.
// ClickHouse accepts FORMAT and SETTINGS once each. The grammar accepts them any number of times.
// APPEND, STDOUT, COMPRESSION and LEVEL are keywords that stay valid as identifiers.
// A quoted `APPEND` is an identifier, so it never matches the keyword.
queryOutputTail
    : (INTO OUTFILE stringLiteral
        APPEND?
        TRUNCATE?
        (AND STDOUT)?
        (COMPRESSION stringLiteral (LEVEL numberLiteral)?)?
      )?
      (FORMAT identifierOrNull | settingsClause)*
    ;

query
    : alterStmt     // DDL
    | attachStmt    // DDL
    | checkStmt
    | createStmt    // DDL
    | describeStmt
    | dropStmt      // DDL
    | existsStmt
    | explainStmt
    | killStmt      // DDL
    | optimizeStmt  // DDL
    | renameStmt    // DDL
    | exchangeStmt  // DDL
    | selectUnionStmt
    | setStmt
    | showStmt
    | systemStmt
    | truncateStmt  // DDL
    | useStmt
    ;

// Statements that do not take the output clauses (INTO OUTFILE, FORMAT) of a query with output.
queryWithoutOutput
    : alterHandlerStmt    // DDL
    | createHandlerStmt   // DDL
    | dropHandlerStmt     // DDL
    | grantStmt           // DCL
    | revokeStmt          // DCL
    ;

// ALTER statement

alterStmt
    : ALTER TABLE tableIdentifier clusterClause? alterTableClause (COMMA alterTableClause)*  # AlterTableStmt
    ;

alterTableClause
    : ADD COLUMN (IF NOT EXISTS)? tableColumnDfnt (AFTER nestedIdentifier)?           # AlterTableClauseAddColumn
    | ADD INDEX (IF NOT EXISTS)? tableIndexDfnt (AFTER nestedIdentifier)?             # AlterTableClauseAddIndex
    | ADD PROJECTION (IF NOT EXISTS)? tableProjectionDfnt (AFTER nestedIdentifier)?   # AlterTableClauseAddProjection
    | ATTACH partitionClause (FROM tableIdentifier)?                                  # AlterTableClauseAttach
    | CLEAR COLUMN (IF EXISTS)? nestedIdentifier (IN partitionClause)?                # AlterTableClauseClearColumn
    | CLEAR INDEX (IF EXISTS)? nestedIdentifier (IN partitionClause)?                 # AlterTableClauseClearIndex
    | CLEAR PROJECTION (IF EXISTS)? nestedIdentifier (IN partitionClause)?            # AlterTableClauseClearProjection
    | COMMENT COLUMN (IF EXISTS)? nestedIdentifier stringLiteral                      # AlterTableClauseComment
    | DELETE WHERE columnExpr                                                         # AlterTableClauseDelete
    | DETACH partitionClause                                                          # AlterTableClauseDetach
    | DROP COLUMN (IF EXISTS)? nestedIdentifier                                       # AlterTableClauseDropColumn
    | DROP INDEX (IF EXISTS)? nestedIdentifier                                        # AlterTableClauseDropIndex
    | DROP PROJECTION (IF EXISTS)? nestedIdentifier                                   # AlterTableClauseDropProjection
    | DROP partitionClause                                                            # AlterTableClauseDropPartition
    | FREEZE partitionClause?                                                         # AlterTableClauseFreezePartition
    | MATERIALIZE INDEX (IF EXISTS)? nestedIdentifier (IN partitionClause)?           # AlterTableClauseMaterializeIndex
    | MATERIALIZE PROJECTION (IF EXISTS)? nestedIdentifier (IN partitionClause)?      # AlterTableClauseMaterializeProjection
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier codecExpr                           # AlterTableClauseModifyCodec
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier COMMENT stringLiteral               # AlterTableClauseModifyComment
    | MODIFY COLUMN (IF EXISTS)? nestedIdentifier REMOVE tableColumnPropertyType      # AlterTableClauseModifyRemove
    | MODIFY COLUMN (IF EXISTS)? tableColumnDfnt                                      # AlterTableClauseModify
    | MODIFY ORDER BY columnExpr                                                      # AlterTableClauseModifyOrderBy
    | MODIFY ttlClause                                                                # AlterTableClauseModifyTTL
    | MOVE partitionClause ( TO DISK stringLiteral
                           | TO VOLUME stringLiteral
                           | TO TABLE tableIdentifier
                           )                                                          # AlterTableClauseMovePartition
    | REMOVE TTL                                                                      # AlterTableClauseRemoveTTL
    | RENAME COLUMN (IF EXISTS)? nestedIdentifier TO nestedIdentifier                 # AlterTableClauseRename
    | REPLACE partitionClause FROM tableIdentifier                                    # AlterTableClauseReplace
    | UPDATE assignmentExprList whereClause                                           # AlterTableClauseUpdate
    ;

assignmentExprList: assignmentExpr (COMMA assignmentExpr)*;
assignmentExpr: nestedIdentifier EQ_SINGLE columnExpr;

tableColumnPropertyType: ALIAS | CODEC | COMMENT | DEFAULT | MATERIALIZED | TTL;

partitionClause
    : PARTITION columnExpr         // actually we expect here any form of tuple of literals
    | PARTITION ID stringLiteral
    ;

// ATTACH statement
attachStmt
    : ATTACH DICTIONARY tableIdentifier clusterClause?  # AttachDictionaryStmt
    ;

// CHECK statement

checkStmt: CHECK TABLE tableIdentifier partitionClause?;

// CREATE statement

createStmt
    : (ATTACH | CREATE) DATABASE (IF NOT EXISTS)? databaseIdentifier clusterClause? engineExpr?                                                                                       # CreateDatabaseStmt
    | (ATTACH | CREATE (OR REPLACE)? | REPLACE) DICTIONARY (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? dictionarySchemaClause dictionaryEngineClause                                          # CreateDictionaryStmt
    | (ATTACH | CREATE) MATERIALIZED VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? (destinationClause | engineClause POPULATE?) subqueryClause  # CreateMaterializedViewStmt
    | (ATTACH | CREATE (OR REPLACE)? | REPLACE) TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? engineClause? tableCommentClause? subqueryClause? tableCommentClause?  # CreateTableStmt
    | (ATTACH | CREATE) (OR REPLACE)? VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? subqueryClause                                              # CreateViewStmt
    ;

dictionarySchemaClause: LPAREN dictionaryAttrDfnt (COMMA dictionaryAttrDfnt)* RPAREN;
// The attributes and the clauses are accepted in any order. ClickHouse accepts each of them at most once.
// The grammar does not check that, because the check needs a semantic predicate in the target language.
dictionaryAttrDfnt
    : identifier columnTypeExpr
    ( DEFAULT literal
    | EXPRESSION columnExpr
    | HIERARCHICAL
    | INJECTIVE
    | IS_OBJECT_ID
    )*
    ;
dictionaryEngineClause
    : dictionaryPrimaryKeyClause?
    ( sourceClause
    | lifetimeClause
    | layoutClause
    | rangeClause
    | dictionarySettingsClause
    )*
    ;
dictionaryPrimaryKeyClause: PRIMARY KEY columnExprList;
dictionaryArgExpr: identifier (identifier (LPAREN RPAREN)? | literal);
sourceClause: SOURCE LPAREN identifier LPAREN dictionaryArgExpr* RPAREN RPAREN;
lifetimeClause: LIFETIME LPAREN ( DECIMAL_LITERAL
                                | MIN DECIMAL_LITERAL MAX DECIMAL_LITERAL
                                | MAX DECIMAL_LITERAL MIN DECIMAL_LITERAL
                                ) RPAREN;
layoutClause: LAYOUT LPAREN identifier LPAREN dictionaryArgExpr* RPAREN RPAREN;
rangeClause: RANGE LPAREN (MIN identifier MAX identifier | MAX identifier MIN identifier) RPAREN;
dictionarySettingsClause: SETTINGS LPAREN settingExprList RPAREN;

clusterClause: ON CLUSTER (identifier | stringLiteral);
uuidClause: UUID stringLiteral;
tableCommentClause: COMMENT stringLiteral;
destinationClause: TO tableIdentifier;
subqueryClause: AS selectUnionStmt;
tableSchemaClause
    : LPAREN tableElementExpr (COMMA tableElementExpr)* RPAREN  # SchemaDescriptionClause
    | AS tableIdentifier                                        # SchemaAsTableClause
    | AS tableFunctionExpr                                      # SchemaAsFunctionClause
    ;
// The clauses are accepted in any order. ClickHouse accepts each of them at most once.
engineClause
    : engineExpr
    ( orderByClause
    | partitionByClause
    | primaryKeyClause
    | sampleByClause
    | ttlClause
    | settingsClause
    )*
    ;
partitionByClause: PARTITION BY columnExpr;
primaryKeyClause: PRIMARY KEY columnExpr;
sampleByClause: SAMPLE BY columnExpr;
ttlClause: TTL ttlExpr (COMMA ttlExpr)*;

engineExpr: ENGINE EQ_SINGLE? identifierOrNull (LPAREN columnExprList? RPAREN)?;
tableElementExpr
    : tableColumnDfnt                                                              # TableElementExprColumn
    | CONSTRAINT identifier (CHECK | ASSUME) columnExpr                            # TableElementExprConstraint
    | INDEX tableIndexDfnt                                                         # TableElementExprIndex
    | PROJECTION tableProjectionDfnt                                               # TableElementExprProjection
    ;
// The column modifiers are accepted in any order. ClickHouse accepts each of them at most once.
tableColumnDfnt
    : nestedIdentifier columnTypeExpr tableColumnPropertyExpr? tableColumnModifierExpr*
    | nestedIdentifier columnTypeExpr? tableColumnPropertyExpr tableColumnModifierExpr*
    ;
tableColumnPropertyExpr
    : (DEFAULT | MATERIALIZED | ALIAS) columnExpr
    | EPHEMERAL columnExpr?
    ;
tableColumnModifierExpr: columnCommentExpr | codecExpr | columnStatisticsExpr | columnTtlExpr | columnSettingsExpr;
columnCommentExpr: COMMENT stringLiteral;
columnStatisticsExpr: STATISTICS LPAREN columnExprList RPAREN;
columnTtlExpr: TTL columnExpr;
columnSettingsExpr: SETTINGS LPAREN settingExprList RPAREN;
tableIndexDfnt: nestedIdentifier columnExpr TYPE columnTypeExpr GRANULARITY DECIMAL_LITERAL;
tableProjectionDfnt
    : nestedIdentifier projectionSelectStmt projectionSettingsExpr?                 # TableProjectionDfntQuery
    | nestedIdentifier INDEX columnExpr TYPE columnTypeExpr projectionSettingsExpr? # TableProjectionDfntIndex
    ;
projectionSettingsExpr: WITH SETTINGS LPAREN settingExprList RPAREN;
codecExpr: CODEC LPAREN codecArgExpr (COMMA codecArgExpr)* RPAREN;
codecArgExpr: identifier (LPAREN columnExprList? RPAREN)?;
// Parsed by ParserTTLElement. DELETE is the default action, so `expr WHERE cond` means `expr DELETE WHERE cond`.
ttlExpr
    : columnExpr (
        DELETE? whereClause
        | DELETE
        | RECOMPRESS codecExpr
        | TO DISK (IF EXISTS)? stringLiteral
        | TO VOLUME (IF EXISTS)? stringLiteral
        | GROUP BY columnExprList (SET assignmentExprList)?
    )?
    ;

// DESCRIBE statement

describeStmt: (DESCRIBE | DESC) TABLE? tableExpr;

// DROP statement

dropStmt
    : (DETACH | DROP) DATABASE (IF EXISTS)? databaseIdentifier clusterClause? SYNC?                                 # DropDatabaseStmt
    | (DETACH | DROP) (DICTIONARY | TEMPORARY? TABLE | VIEW) (IF EXISTS)? tableIdentifier clusterClause? (NO DELAY | SYNC)?  # DropTableStmt
    ;

// CREATE TOKEN is a query with output, so it reads the output clauses (INTO OUTFILE, FORMAT, SETTINGS) itself.
// VALID and GRANTS are accepted in any order. ClickHouse accepts each of them at most once.
createTokenStmt: CREATE TOKEN (tokenValidClause | tokenGrantsClause)* queryOutputTail;
tokenValidClause
    : VALID UNTIL stringLiteral
    | VALID FOR columnExpr
    ;
tokenGrantsClause: GRANTS LPAREN tokenGrantElement (COMMA tokenGrantElement)* RPAREN;
tokenGrantElement
    : privilegeList ON grantScope
    | sourcePrivilegeList ON sourceScope
    ;

// HTTP handlers. The clauses are in the order of ParserCreateHandlerQuery and ParserDropHandlerQuery.
// URL is required for CREATE and optional for ALTER.
// The AS query is parsed by ParserQuery, so the output clauses belong to that query, not to the handler.
createHandlerStmt
    : CREATE HANDLER (IF NOT EXISTS)? identifier clusterClause?
      handlerProtocolClause? handlerUrlClause handlerMethodsClause? handlerTypeClause? handlerAsClause?
    ;
alterHandlerStmt
    : ALTER HANDLER identifier clusterClause?
      handlerProtocolClause? handlerUrlClause? handlerMethodsClause? handlerTypeClause? handlerAsClause?
    ;
dropHandlerStmt: DROP HANDLER (IF EXISTS)? identifier clusterClause?;
handlerProtocolClause: PROTOCOL identifier;  // PROTOCOL ANY resets the protocol, and ANY is a keyword here
handlerUrlClause: URL (PREFIX | REGEXP)? stringLiteral;
handlerMethodsClause: METHODS LPAREN handlerMethod (COMMA handlerMethod)* RPAREN;
handlerMethod: GET | POST | PUT | DELETE;
handlerTypeClause: TYPE identifier;
handlerAsClause: AS handlerInnerQuery;
// One pair of parentheses is a fallback. Parentheses of a SELECT stay in selectUnionStmt.
handlerInnerQuery
    : handlerInnerQueryBody
    | LPAREN handlerInnerQueryBody RPAREN
    ;
handlerInnerQueryBody
    : query queryOutputTail
    | createTokenStmt
    | queryWithoutOutput
    | embeddedInsertStmt
    ;
// The inline data of an INSERT is not part of a handler: the data comes from the HTTP body.
embeddedInsertStmt: INSERT INTO TABLE? (tableIdentifier | FUNCTION tableFunctionExpr) columnsClause? embeddedDataClause;
embeddedDataClause
    : FORMAT identifier
    | VALUES
    | selectUnionStmt (FORMAT identifier)?
    ;

// EXISTS statement

existsStmt
    : EXISTS DATABASE databaseIdentifier                             # ExistsDatabaseStmt
    | EXISTS (DICTIONARY | TEMPORARY? TABLE | VIEW)? tableIdentifier # ExistsTableStmt
    ;

// EXPLAIN statement

explainStmt
    : EXPLAIN (AST | SYNTAX | QUERY TREE | PLAN | PIPELINE | ESTIMATE | TABLE OVERRIDE)? settingExprList? selectUnionStmt
    ;

// GRANT and REVOKE statements

grantStmt
    : GRANT clusterClause? privilegeList ON grantScope TO granteeList grantOptionList        # GrantPrivilegeStmt
    | GRANT clusterClause? sourcePrivilegeList ON sourceScope TO granteeList grantOptionList # GrantSourcePrivilegeStmt
    | GRANT clusterClause? roleList TO granteeList grantAdminOptionList                      # GrantRoleStmt
    ;

revokeStmt
    : REVOKE clusterClause? privilegeList ON grantScope FROM granteeList           # RevokePrivilegeStmt
    | REVOKE clusterClause? sourcePrivilegeList ON sourceScope FROM granteeList    # RevokeSourcePrivilegeStmt
    | REVOKE clusterClause? (ADMIN OPTION FOR)? roleList FROM granteeList          # RevokeRoleStmt
    ;

// List of privileges
privilegeList: privilege (COMMA privilege)*;

// Single privilege with optional column list
privilege: privilegeName (LPAREN columnList RPAREN)?;

// Privilege names. This is the list of access types in src/Access/Common/AccessType.h
// that the grammar knows. It is not complete, and it does not list every alias.
// See https://clickhouse.com/docs/sql-reference/statements/grant#privileges
privilegeName
    : SELECT
    | INSERT
    | ALTER alterPrivilege?
    | CREATE createPrivilege?
    | DROP dropPrivilege?
    | TRUNCATE
    | OPTIMIZE
    | SHOW showPrivilege?
    | KILL QUERY
    | ACCESS MANAGEMENT
    | SYSTEM systemPrivilege?
    | INTROSPECTION
    | SOURCES
    | DICTGET
    | ALL PRIVILEGES?
    | NONE
    | NAMED COLLECTION ADMIN?
    | TABLE ENGINE
    | UNDROP TABLE
    | MOVE PARTITION BETWEEN SHARDS
    | SET DEFINER
    // Deprecated source-as-privilege forms; modern syntax is `READ|WRITE ON <source>`
    | S3
    | AZURE
    | HDFS
    | MONGO
    | MYSQL
    | ODBC
    | JDBC
    | POSTGRES
    | REDIS
    | SQLITE
    | REMOTE
    | URL
    | KAFKA
    ;

// ALTER sub-privileges
alterPrivilege
    : TABLE
    | VIEW
    | DATABASE
    | COLUMN
    | INDEX
    | CONSTRAINT
    | TTL
    | SETTINGS
    | MOVE PARTITION
    | FETCH PARTITION
    | FREEZE PARTITION
    | UPDATE
    | DELETE
    | ADD COLUMN
    | DROP COLUMN
    | MODIFY COLUMN
    | COMMENT COLUMN
    | CLEAR COLUMN
    | RENAME COLUMN
    | MATERIALIZE COLUMN
    | ADD INDEX
    | DROP INDEX
    | MATERIALIZE INDEX
    | CLEAR INDEX
    | ADD CONSTRAINT
    | DROP CONSTRAINT
    | MODIFY TTL
    | MATERIALIZE TTL
    | MODIFY SETTING
    | MODIFY COMMENT
    | USER
    | ROLE
    | ROW POLICY
    | QUOTA
    | SETTINGS PROFILE
    | HANDLER
    ;

// CREATE sub-privileges
createPrivilege
    : TABLE
    | VIEW
    | DATABASE
    | DICTIONARY
    | FUNCTION
    | TEMPORARY TABLE
    | ARBITRARY TEMPORARY TABLE
    | USER
    | ROLE
    | ROW POLICY
    | QUOTA
    | SETTINGS PROFILE
    | NAMED COLLECTION
    | TOKEN
    | HANDLER
    ;

// DROP sub-privileges
dropPrivilege
    : TABLE
    | VIEW
    | DATABASE
    | DICTIONARY
    | FUNCTION
    | USER
    | ROLE
    | ROW POLICY
    | QUOTA
    | SETTINGS PROFILE
    | NAMED COLLECTION
    | HANDLER
    ;

// SHOW sub-privileges
showPrivilege
    : DATABASES
    | TABLES
    | COLUMNS
    | DICTIONARIES
    | USERS
    | ROLES
    | ROW POLICIES
    | QUOTAS
    | SETTINGS PROFILES
    | ACCESS
    | NAMED COLLECTIONS
    | HANDLERS
    | HANDLER
    ;

// SYSTEM sub-privileges
systemPrivilege
    : SHUTDOWN
    | DROP CACHE
    | DROP DNS CACHE
    | DROP MARK CACHE
    | DROP UNCOMPRESSED CACHE
    | DROP MMAP CACHE
    | DROP COMPILED EXPRESSION CACHE
    | RELOAD
    | RELOAD CONFIG
    | RELOAD DICTIONARY
    | RELOAD EMBEDDED DICTIONARIES
    | RELOAD FUNCTION
    | RELOAD FUNCTIONS
    | RESTART REPLICA
    | RESTORE REPLICA
    | SYNC REPLICA
    | SYNC DATABASE REPLICA
    | FLUSH
    | FLUSH DISTRIBUTED
    | FLUSH LOGS
    | FLUSH ASYNC INSERT QUEUE
    | MERGES
    | TTL MERGES
    | FETCHES
    | MOVES
    | DISTRIBUTED SENDS
    | REPLICATED SENDS
    | REPLICATION QUEUES
    | STOP MERGES
    | STOP TTL MERGES
    | STOP FETCHES
    | STOP MOVES
    | STOP DISTRIBUTED SENDS
    | STOP REPLICATED SENDS
    | STOP REPLICATION QUEUES
    | START MERGES
    | START TTL MERGES
    | START FETCHES
    | START MOVES
    | START DISTRIBUTED SENDS
    | START REPLICATED SENDS
    | START REPLICATION QUEUES
    ;

// Column list for column-level privileges
columnList: identifier (COMMA identifier)*;

// Scope: db.table, db.*, *.*, table, *
grantScope
    : identifier DOT identifier    // db.table
    | identifier DOT ASTERISK      // db.*
    | ASTERISK DOT ASTERISK        // *.*
    | identifier                   // table only
    | ASTERISK                     // *
    ;

// Source-level privileges (table functions and external storages):
//   GRANT READ ON S3 TO user                          -- all S3 paths
//   GRANT READ ON S3('s3://bucket/prefix/*') TO user  -- restricted to a regular expression
//   GRANT WRITE ON * TO user                          -- every source
sourcePrivilegeList: sourcePrivilege (COMMA sourcePrivilege)*;
sourcePrivilege: READ | WRITE;
sourceScope
    : sourceName (LPAREN stringLiteral RPAREN)?
    | ASTERISK
    ;
// A source is one of the names in AccessTypeObjects::Source: FILE, URL, REMOTE, MONGO, REDIS, MYSQL,
// POSTGRES, SQLITE, ODBC, JDBC, HDFS, S3, HIVE, AZURE, KAFKA, NATS, RABBITMQ, YTSAURUS, ARROW_FLIGHT,
// BIGQUERY, ELASTICSEARCH, DISK, or the alias of a table engine such as MongoDB.
// The grammar accepts any identifier here, because most of these names are not lexer keywords.
sourceName: identifier;

// List of grantees (users or roles)
granteeList: grantee (COMMA grantee)*;

// Single grantee
grantee
    : identifier
    | CURRENT_USER
    ;

// Role list for role grants
roleList: identifier (COMMA identifier)*;

// GRANT options: WITH GRANT OPTION, then WITH REPLACE OPTION
grantOptionList: (WITH GRANT OPTION)? (WITH REPLACE OPTION)?;

// ADMIN options for role grants: WITH ADMIN OPTION, then WITH REPLACE OPTION
grantAdminOptionList: (WITH ADMIN OPTION)? (WITH REPLACE OPTION)?;

// INSERT statement

// INSERT statement

insertStmt: INSERT INTO TABLE? (tableIdentifier | FUNCTION tableFunctionExpr) columnsClause? dataClause;

columnsClause: LPAREN nestedIdentifier (COMMA nestedIdentifier)* RPAREN;
dataClause
    : FORMAT identifier                                                         # DataClauseFormat
    | VALUES assignmentValues (COMMA assignmentValues)*                         # DataClauseValues
    | selectUnionStmt SEMICOLON? EOF                                            # DataClauseSelect
    ;

assignmentValues
    : LPAREN assignmentValue (COMMA assignmentValue)* RPAREN
    | LPAREN RPAREN
    ;
assignmentValue
    : literal
    ;

// DELETE statement

deleteStmt
    : DELETE FROM nestedIdentifier clusterClause? inPartitionClause? whereClause
    ;

inPartitionClause
    : IN PARTITION columnExpr
    ;

// UPDATE statement

updateStmt
    : UPDATE nestedIdentifier SET assignmentExprList clusterClause? inPartitionClause? whereClause
    ;

// KILL statement

killStmt
    : KILL MUTATION clusterClause? whereClause (SYNC | ASYNC | TEST)?  # KillMutationStmt
    ;

// OPTIMIZE statement

optimizeStmt: OPTIMIZE TABLE tableIdentifier clusterClause? partitionClause? FINAL? DEDUPLICATE?;

// RENAME statement

renameStmt: RENAME renameEntityClause clusterClause?;

renameEntityClause
    : TABLE? tableIdentifier TO tableIdentifier (COMMA tableIdentifier TO tableIdentifier)*
    | DATABASE databaseIdentifier TO databaseIdentifier (COMMA databaseIdentifier TO databaseIdentifier)*
    | DICTIONARY dictionaryIdentifier TO dictionaryIdentifier (COMMA dictionaryIdentifier TO dictionaryIdentifier)*
    ;

// EXCHANGE statement

exchangeStmt
    : EXCHANGE (TABLES | DICTIONARIES)
      tableIdentifier AND tableIdentifier
      (COMMA tableIdentifier AND tableIdentifier)*
      clusterClause?
    ;

// PROJECTION SELECT statement

projectionSelectStmt:
    LPAREN
    withClause?
    SELECT columnExprList
    groupByClause?
    projectionOrderByClause?
    RPAREN
    ;

// SELECT statement

selectUnionStmt: selectStmtWithParens (UNION (ALL | DISTINCT) selectStmtWithParens)*;
selectStmtWithParens: selectStmt | LPAREN selectUnionStmt RPAREN | LPAREN explainSelectSubquery RPAREN;
// ParserSubquery accepts a parenthesized EXPLAIN only when the explained query is a SELECT.
explainSelectSubquery: EXPLAIN (AST | SYNTAX | QUERY TREE | PLAN | PIPELINE | ESTIMATE)? settingExprList? selectUnionStmt;
selectStmt:
    withClause?
    SELECT DISTINCT? topClause? columnExprList
    fromClause?
    arrayJoinClause?
    windowClause?
    prewhereClause?
    whereClause?
    groupByClause? (WITH (CUBE | ROLLUP))? (WITH TOTALS)?
    havingClause?
    orderByClause?
    interpolateClause?
    limitByClause?
    limitClause?
    settingsClause?
    ;

withClause: WITH withExprList;
withExprList: withExpr (COMMA withExpr)*;
withExpr
    : RECURSIVE? identifier AS LPAREN selectUnionStmt RPAREN  # WithExprSubquery
    | columnExpr AS identifier                                # WithExprExpression
    ;
topClause: TOP DECIMAL_LITERAL (WITH TIES)?;
fromClause: FROM joinExpr;
arrayJoinClause: (LEFT | INNER)? ARRAY JOIN columnExprList;
windowClause: WINDOW identifier AS LPAREN windowExpr RPAREN;
prewhereClause: PREWHERE columnExpr;
whereClause: WHERE columnExpr;
groupByClause
  : GROUP BY ALL                                           # GroupByClauseAll
  | GROUP BY (CUBE | ROLLUP) LPAREN columnExprList RPAREN  # GroupByClauseCubeOrRollup
  | GROUP BY GROUPING SETS LPAREN columnExprList RPAREN    # GroupByClauseGroupingSets
  | GROUP BY columnExprList                                # GroupByClauseSimple
  ;
havingClause: HAVING columnExpr;
orderByClause: ORDER BY orderExprList;
interpolateClause: INTERPOLATE LPAREN columnExprList RPAREN;
projectionOrderByClause: ORDER BY columnExprList;
limitByClause: LIMIT limitExpr BY columnExprList;
limitClause: LIMIT limitExpr (WITH TIES)?;
settingsClause: SETTINGS settingExprList;

joinExpr
    : joinExpr (GLOBAL | LOCAL)? joinOp? JOIN joinExpr joinConstraintClause  # JoinExprOp
    | joinExpr joinOpCross joinExpr                                          # JoinExprCrossOp
    | tableExpr FINAL? sampleClause?                                         # JoinExprTable
    | LPAREN joinExpr RPAREN                                                 # JoinExprParens
    ;
joinOp
    : ((ALL | ANY | ASOF)? INNER | INNER (ALL | ANY | ASOF)? | (ALL | ANY | ASOF))  # JoinOpInner
    | ( (SEMI | ALL | ANTI | ANY | ASOF)? (LEFT | RIGHT) OUTER?
      | (LEFT | RIGHT) OUTER? (SEMI | ALL | ANTI | ANY | ASOF)?
      )                                                                             # JoinOpLeftRight
    | ((ALL | ANY)? FULL OUTER? | FULL OUTER? (ALL | ANY)?)                         # JoinOpFull
    ;
joinOpCross
    : (GLOBAL|LOCAL)? CROSS JOIN
    | COMMA
    ;
joinConstraintClause
    : ON columnExprList
    | USING LPAREN columnExprList RPAREN
    | USING columnExprList
    ;

sampleClause: SAMPLE ratioExpr (OFFSET ratioExpr)?;
limitExpr: columnExpr ((COMMA | OFFSET) columnExpr)?;
orderExprList: orderExpr (COMMA orderExpr)*;
orderExpr: columnExpr (ASCENDING | DESCENDING | DESC)? (NULLS (FIRST | LAST))? (COLLATE stringLiteral)? (WITH FILL (FROM columnExpr)? (TO columnExpr)? (STEP columnExpr)?)?;
ratioExpr: numberLiteral (SLASH numberLiteral)?;
settingExprList: settingExpr (COMMA settingExpr)*;
settingExpr: identifier EQ_SINGLE literal;

windowExpr: winPartitionByClause? winOrderByClause? winFrameClause?;
winPartitionByClause: PARTITION BY columnExprList;
winOrderByClause: ORDER BY orderExprList;
winFrameClause: (ROWS | RANGE) winFrameExtend;
winFrameExtend
    : winFrameBound                             # frameStart
    | BETWEEN winFrameBound AND winFrameBound   # frameBetween
    ;
winFrameBound: (CURRENT ROW | UNBOUNDED PRECEDING | UNBOUNDED FOLLOWING | numberLiteral PRECEDING | numberLiteral FOLLOWING);
//rangeClause: RANGE LPAREN (MIN identifier MAX identifier | MAX identifier MIN identifier) RPAREN;


// SET statement

setStmt: SET settingExprList;

// SHOW statements

showStmt
    : SHOW CREATE? DATABASE databaseIdentifier                                                                                                                                                        # showCreateDatabaseStmt
    | SHOW CREATE DICTIONARY tableIdentifier                                                                                                                                                          # showCreateDictionaryStmt
    | SHOW CREATE? VIEW tableIdentifier                                                                                                                                                               # showCreateViewStmt
    | SHOW DATABASES ((NOT? (LIKE | ILIKE) stringLiteral) | WHERE columnExpr)? (LIMIT DECIMAL_LITERAL)?                                                                                               # showDatabasesStmt
    | SHOW DICTIONARIES (FROM databaseIdentifier)? ((NOT? (LIKE | ILIKE) stringLiteral) | WHERE columnExpr)? (LIMIT DECIMAL_LITERAL)?                                                                 # showDictionariesStmt
    | SHOW FULL? TEMPORARY? TABLES ((FROM | IN) databaseIdentifier)? ((NOT? (LIKE | ILIKE) stringLiteral) | WHERE columnExpr)? (LIMIT DECIMAL_LITERAL)?                                               # showTablesStmt
    | SHOW EXTENDED? FULL? (COLUMNS | FIELDS) (FROM | IN) (tableIdentifier | (identifier (FROM | IN) identifier)) ((NOT? (LIKE | ILIKE) stringLiteral) | WHERE columnExpr)? (LIMIT DECIMAL_LITERAL)?  # showColumnsStmt
    | SHOW EXTENDED? (INDEX | INDEXES | INDICES | KEYS) (FROM | IN) (tableIdentifier | (identifier (FROM | IN) identifier)) (WHERE columnExpr)?                                                       # showIndexStmt
    | SHOW PROCESSLIST                                                                                                                                                                                # showProcessListStmt
    | SHOW GRANTS (FOR identifier (COMMA identifier)*)? (WITH IMPLICIT)? (FINAL)?                                                                                                                     # showGrantsStmt
    | SHOW CREATE USER ((identifier (COMMA identifier)*) | CURRENT_USER)?                                                                                                                             # showCreateUserStmt
    | SHOW CREATE ROLE identifier (COMMA identifier)*                                                                                                                                                 # showCreateRoleStmt
    | SHOW CREATE ROW? POLICY identifier ON tableIdentifier (COMMA tableIdentifier)*                                                                                                                  # showCreatePolicyStmt
    | SHOW CREATE QUOTA ((identifier (COMMA identifier)*) | CURRENT)                                                                                                                                  # showCreateQuotaStmt
    | SHOW CREATE SETTINGS? PROFILE identifier (COMMA identifier)*                                                                                                                                    # showCreateProfileStmt
    | SHOW USERS                                                                                                                                                                                      # showUsersStmt
    | SHOW (CURRENT | ENABLED)? ROLES                                                                                                                                                                 # showRolesStmt
    | SHOW SETTINGS? PROFILES                                                                                                                                                                         # showProfilesStmt
    | SHOW ROW? POLICIES (ON tableIdentifier)?                                                                                                                                                        # showPoliciesStmt
    | SHOW QUOTAS                                                                                                                                                                                     # showQuotasStmt
    | SHOW CURRENT? QUOTA                                                                                                                                                                             # showQuotaStmt
    | SHOW ACCESS                                                                                                                                                                                     # showAccessStmt
    | SHOW CLUSTER stringLiteral                                                                                                                                                                      # showClusterStmt
    | SHOW CLUSTERS (NOT? (LIKE | ILIKE) stringLiteral)? (LIMIT DECIMAL_LITERAL)?                                                                                                                     # showClustersStmt
    | SHOW CHANGED? SETTINGS (LIKE | ILIKE) stringLiteral                                                                                                                                             # showSettingsStmt
    | SHOW SETTING stringLiteral                                                                                                                                                                      # showSettingStmt
    | SHOW FILESYSTEM CACHES                                                                                                                                                                          # showFilesystemCaches
    | SHOW ENGINES                                                                                                                                                                                    # showEnginesStmt
    | SHOW FUNCTIONS ((LIKE | ILIKE) stringLiteral)?                                                                                                                                                  # showFunctionsStmt
    | SHOW MERGES (NOT? (LIKE | ILIKE) stringLiteral)? (LIMIT DECIMAL_LITERAL)?                                                                                                                       # showMergesStmt
    | SHOW PRIVILEGES                                                                                                                                                                                 # showPrivilegesStmt
    | SHOW (TABLE | CREATE TEMPORARY? TABLE?)? tableIdentifier                                                                                                                                        # showCreateTableStmt
    ;

// SYSTEM statements

systemStmt
    : SYSTEM FLUSH DISTRIBUTED tableIdentifier
    | SYSTEM FLUSH LOGS
    | SYSTEM RELOAD DICTIONARIES
    | SYSTEM RELOAD DICTIONARY tableIdentifier
    | SYSTEM (START | STOP) (DISTRIBUTED SENDS | FETCHES | TTL? MERGES) tableIdentifier
    | SYSTEM (START | STOP) REPLICATED SENDS
    | SYSTEM SYNC REPLICA tableIdentifier
    ;

// TRUNCATE statements

truncateStmt: TRUNCATE TEMPORARY? TABLE? (IF EXISTS)? tableIdentifier clusterClause?;

// USE statement

useStmt: USE databaseIdentifier;



// Columns

columnTypeExpr
    : identifier                                                                             # ColumnTypeExprSimple   // UInt64
    | identifier LPAREN identifier columnTypeExpr (COMMA identifier columnTypeExpr)* RPAREN  # ColumnTypeExprNested   // Nested
    | identifier LPAREN enumValue (COMMA enumValue)* RPAREN                                  # ColumnTypeExprEnum     // Enum
    | identifier LPAREN columnTypeExpr (COMMA columnTypeExpr)* RPAREN                        # ColumnTypeExprComplex  // Array, Tuple
    | identifier LPAREN columnExprList? RPAREN                                               # ColumnTypeExprParam    // FixedString(N)
    ;
columnExprList: columnsExpr (COMMA columnsExpr)*;
columnsExpr
    : (tableIdentifier DOT)? ASTERISK columnExceptExpr?                                 # ColumnsExprAsterisk
    | LPAREN selectUnionStmt RPAREN                                                     # ColumnsExprSubquery
    // NOTE: asterisk and subquery goes before |columnExpr| so that we can mark them as multi-column expressions.
    | columnExpr                                                                        # ColumnsExprColumn
    ;
columnExpr
    : CASE columnExpr? (WHEN columnExpr THEN columnExpr)+ (ELSE columnExpr)? END          # ColumnExprCase
    | CAST LPAREN columnExpr AS columnTypeExpr RPAREN                                     # ColumnExprCast
    | columnExpr DOUBLE_COLON columnTypeExpr                                              # ColumnExprCastSymbol
    | DATE stringLiteral                                                                  # ColumnExprDate
    | EXTRACT LPAREN interval FROM columnExpr RPAREN                                      # ColumnExprExtract
    | INTERVAL columnExpr interval                                                        # ColumnExprInterval
    | SUBSTRING LPAREN columnExpr FROM columnExpr (FOR columnExpr)? RPAREN                # ColumnExprSubstring
    | TIMESTAMP stringLiteral                                                             # ColumnExprTimestamp
    | TRIM LPAREN (BOTH | LEADING | TRAILING) stringLiteral  FROM columnExpr RPAREN       # ColumnExprTrim
    // TODO(ilezhankin): `BETWEEN a AND b AND c` is parsed in a wrong way: `BETWEEN (a AND b) AND c`
    | columnExpr NOT? BETWEEN columnExpr AND columnExpr                                   # ColumnExprBetween
    | NOT columnExpr                                                                      # ColumnExprNot
    | identifier (LPAREN columnExprList? RPAREN) OVER LPAREN windowExpr RPAREN            # ColumnExprWinFunction
    | identifier (LPAREN columnExprList? RPAREN) OVER identifier                          # ColumnExprWinFunctionTarget
    | identifier (LPAREN columnExprList? RPAREN)? LPAREN DISTINCT? columnArgList? RPAREN  # ColumnExprFunction
    | literal                                                                             # ColumnExprLiteral

    | columnExpr LBRACKET columnExpr RBRACKET                                             # ColumnExprArrayAccess
    | columnExpr DOT (DECIMAL_LITERAL | stringLiteral | identifier)                       # ColumnExprTupleAccess
    | DASH columnExpr                                                                     # ColumnExprNegate
    | columnExpr ( ASTERISK                                                               // multiply
                 | SLASH                                                                  // divide
                 | PERCENT                                                                // modulo
                 ) columnExpr                                                             # ColumnExprPrecedence1
    | columnExpr ( PLUS                                                                   // plus
                 | DASH                                                                   // minus
                 | CONCAT                                                                 // concat
                 ) columnExpr                                                             # ColumnExprPrecedence2
    | columnExpr ( EQ_DOUBLE                                                              // equals
                 | EQ_SINGLE                                                              // equals
                 | NOT_EQ                                                                 // notEquals
                 | LE                                                                     // lessOrEquals
                 | GE                                                                     // greaterOrEquals
                 | LT                                                                     // less
                 | GT                                                                     // greater
                 | GLOBAL? NOT? IN                                                        // in, notIn, globalIn, globalNotIn
                 | NOT? (LIKE | ILIKE)                                                    // like, notLike, ilike, notILike
                 ) columnExpr                                                             # ColumnExprPrecedence3
    | columnExpr IS NOT? NULL_SQL                                                         # ColumnExprIsNull
    | columnExpr AND columnExpr                                                           # ColumnExprAnd
    | columnExpr OR columnExpr                                                            # ColumnExprOr
    | <assoc=right> columnExpr QUESTIONMARK columnExpr COLON columnExpr                          # ColumnExprTernaryOp
    | columnExpr (alias | AS identifier)                                                  # ColumnExprAlias

    | (tableIdentifier DOT)? ASTERISK                                                     # ColumnExprAsterisk  // single-column only
    | LPAREN selectUnionStmt RPAREN                                                       # ColumnExprSubquery  // single-column only
    | LPAREN explainSelectSubquery RPAREN                                                 # ColumnExprExplainSubquery  // single-column only
    | LPAREN columnExpr RPAREN                                                            # ColumnExprParens    // single-column only
    | LPAREN columnExprList? RPAREN                                                       # ColumnExprTuple
    | LBRACKET columnExprList? RBRACKET                                                   # ColumnExprArray
    | columnIdentifier                                                                    # ColumnExprIdentifier
    ;
columnArgList: columnArgExpr (COMMA columnArgExpr)*;
columnArgExpr: columnLambdaExpr | columnExpr;
columnLambdaExpr:
    ( LPAREN identifier (COMMA identifier)* RPAREN
    |        identifier (COMMA identifier)*
    )
    ARROW columnExpr
    ;
columnIdentifier: (tableIdentifier DOT)? nestedIdentifier;
nestedIdentifier: identifier (DOT identifier)*;
columnExceptExpr
    : EXCEPT (stringLiteral | (LPAREN stringLiteral RPAREN))                # columnExceptExprRegexp
    | EXCEPT (identifier | (LPAREN identifier (COMMA identifier)* RPAREN))  # columnExceptExprIdentifiers
    ;

// Tables

tableExpr
    : tableIdentifier                      # TableExprIdentifier
    | tableFunctionExpr                    # TableExprFunction
    | LPAREN selectUnionStmt RPAREN        # TableExprSubquery
    | LPAREN explainSelectSubquery RPAREN  # TableExprExplainSubquery
    | tableExpr (alias | AS identifier)    # TableExprAlias
    ;
tableFunctionExpr: identifier LPAREN tableArgList? RPAREN;
tableIdentifier: (databaseIdentifier DOT)? identifier;
tableArgList: tableArgExpr (COMMA tableArgExpr)*;
tableArgExpr
    : tableFunctionExpr
    | literal
    | nestedIdentifier
    ;

// Databases

databaseIdentifier: identifier;

// Dictionaries

dictionaryIdentifier: (databaseIdentifier DOT)? identifier;

// Basics

floatingLiteral
    : FLOATING_LITERAL
    | DOT (DECIMAL_LITERAL | OCTAL_LITERAL)
    | DECIMAL_LITERAL DOT (DECIMAL_LITERAL | OCTAL_LITERAL)?  // can't move this to the lexer or it will break nested tuple access: t.1.2
    ;
numberLiteral: (PLUS | DASH)? (floatingLiteral | OCTAL_LITERAL | DECIMAL_LITERAL | HEXADECIMAL_NUMERIC_LITERAL | BINARY_NUMERIC_LITERAL | INF | NAN_SQL);
stringLiteral: HEXADECIMAL_STRING_LITERAL | BINARY_STRING_LITERAL | STRING_LITERAL;
literal
    : numberLiteral
    | JSON_FALSE
    | JSON_TRUE
    | stringLiteral
    | NULL_SQL
    ;
interval: NANOSECOND | MICROSECOND | MILLISECOND | SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR;
keyword
    // except NULL_SQL, INF, NAN_SQL
    : ACCESS | ADD | AFTER | ALIAS | ALL | ALTER | AND | ANTI | ANY | ARRAY | AS | ASCENDING | ASOF | AST | ASYNC | ATTACH | BETWEEN | BOTH | BY | CACHES | CASE
    | CAST | CHECK | CLEAR | CLUSTER | CLUSTERS | CODEC | COLLATE | COLUMN | COLUMNS | COMMENT | CONSTRAINT | CREATE | CROSS | CUBE | CURRENT | CURRENT_USER | CHANGED | DATABASE
    | DATABASES | DATE | DAY | DEDUPLICATE | DEFAULT | DELAY | DELETE | DESC | DESCENDING | DESCRIBE | DETACH | DICTIONARIES | DICTIONARY | DISK
    | DISTINCT | DISTRIBUTED | DROP | ELSE | ENABLED | END | ENGINE | ENGINES | ESTIMATE | EXCEPT | EXISTS | EXPLAIN | EXPRESSION | EXTENDED | EXTRACT | FETCHES | FIELDS | FILESYSTEM | FILL | FINAL | FIRST
    | FLUSH | FOLLOWING | FOR | FORMAT | FREEZE | FROM | FULL | FUNCTION | FUNCTIONS | GLOBAL | GRANULARITY | GRANTS | GROUP | GROUPING | HAVING | HIERARCHICAL | HOUR | ID
    | IF | ILIKE | IMPLICIT | IN | INDEX | INDEXES | INDICES | INJECTIVE | INNER | INSERT | INTERPOLATE | INTERVAL | INTO | IS | IS_OBJECT_ID | JOIN | JSON_FALSE | JSON_TRUE | KEY | KEYS
    | KILL | LAST | LAYOUT | LEADING | LEFT | LIFETIME | LIKE | LIMIT | LIVE | LOCAL | LOGS | MATERIALIZE | MATERIALIZED | MAX | MERGES
    | MICROSECOND | MILLISECOND | MIN | MINUTE | MODIFY | MOVE | MUTATION | NO | NOT | NULLS | OFFSET | ON | OPTIMIZE | OR | ORDER | OUTER | OUTFILE | OVER | OVERRIDE | PARTITION | PIPELINE | PLAN
    | POLICY | POLICIES | POPULATE | PRECEDING | PREWHERE | PRIMARY | PRIVILEGES | PROCESSLIST | PROFILE | PROFILES | PROJECTION | QUARTER | QUOTA | QUOTAS | RANGE | RECURSIVE | RELOAD | REMOVE | RENAME | REPLACE | REPLICA | REPLICATED | RIGHT | ROLE | ROLES | ROLLUP | ROW
    | ROWS | SAMPLE | SECOND | SELECT | SEMI | SENDS | SET | SETTING | SETTINGS | SHOW | SOURCE | START | STOP | SUBSTRING | SYNC | SYNTAX | SYSTEM | STEP | TABLE
    | TABLES | TEMPORARY | TEST | THEN | TIES | TIMEOUT | TIMESTAMP | TO | TOP | TOTALS | TRAILING | TREE | TRIM | TRUNCATE | TTL | TYPE
    | UNBOUNDED | UNION | UPDATE | USE | USER | USERS | USING | UUID | VALUES | VIEW | VOLUME | WEEK | WHEN | WHERE | WINDOW | WITH | YEAR
    | ARBITRARY | CACHE | COLLECTION | COLLECTIONS | COMPILED | CONFIG | DNS | EMBEDDED | FETCH | MARK | MMAP | UNCOMPRESSED
    | ADMIN | APPEND | ASSUME | AZURE | COMPRESSION | DEFINER | DICTGET | EPHEMERAL | EXCHANGE | GET | GRANT | HANDLER | HANDLERS | HDFS
    | INTROSPECTION | JDBC | KAFKA | LEVEL | MANAGEMENT | METHODS | MONGO | MOVES | MYSQL | NAMED | NONE | ODBC | OPTION | POST | POSTGRES | PREFIX
    | PROTOCOL | PUT | QUERY | QUEUE | QUEUES | READ | RECOMPRESS | REDIS | REGEXP | REMOTE | REPLICATION | RESET | RESTART | RESTORE | REVOKE | S3
    | SHARDS | SHUTDOWN | SOURCES | SQLITE | STATISTICS | STDOUT | TOKEN | UNDROP | UNTIL | URL | VALID | WRITE
    ;
keywordForAlias
    : AFTER | ALIAS | ALTER | AST | ASYNC | ATTACH | BOTH | CASE | CAST | CHECK | CLEAR | CLUSTER | CODEC
    | COLUMN | COMMENT | CONSTRAINT | CREATE | CUBE | CURRENT | DATABASE | DATABASES | DATE | DEDUPLICATE | DEFAULT | DELAY
    | DESCRIBE | DETACH | DICTIONARIES | DICTIONARY | DISK | DISTRIBUTED | DROP | ENGINE
    | EXISTS | EXPLAIN | EXPRESSION | EXTRACT | FETCHES | FLUSH | FOLLOWING | FREEZE | FUNCTION | GRANULARITY | HIERARCHICAL
    | ID | IF | INDEX | INJECTIVE | INSERT | IS_OBJECT_ID | KEY | KILL | LAYOUT | LEADING | LIFETIME | LIVE
    | LOGS | MATERIALIZE | MATERIALIZED | MAX | MERGES | MIN | MODIFY | MOVE | MUTATION | NO | OPTIMIZE | OUTFILE | OVER
    | PARTITION | POPULATE | PRECEDING | PRIMARY | RANGE | RELOAD | REMOVE | RENAME | REPLACE | REPLICA | REPLICATED | ROLLUP | ROW
    | SELECT | SENDS | SET | SHOW | SOURCE | START | STOP | SUBSTRING | SYNC | SYNTAX | SYSTEM | TABLE | TABLES | TEMPORARY
    | TEST | TIES | TIMEOUT | TIMESTAMP | TOTALS | TRAILING | TRIM | TRUNCATE | TTL | TYPE | UNBOUNDED | UPDATE
    | USE | UUID | VALUES | VIEW | VOLUME
    | ARBITRARY | CACHE | COLLECTION | COLLECTIONS | COMPILED | CONFIG | DNS | EMBEDDED | FETCH | MARK | MMAP | UNCOMPRESSED
    | ADMIN | APPEND | ASSUME | AZURE | COMPRESSION | DEFINER | DICTGET | EPHEMERAL | EXCHANGE | GET | GRANT | HANDLER | HANDLERS | HDFS
    | INTROSPECTION | JDBC | KAFKA | LEVEL | MANAGEMENT | METHODS | MONGO | MOVES | MYSQL | NAMED | NONE | ODBC | OPTION | POST | POSTGRES | PREFIX
    | PROTOCOL | PUT | QUERY | QUEUE | QUEUES | READ | REDIS | REGEXP | REMOTE | REPLICATION | RESET | RESTART | RESTORE | REVOKE | S3 | SHARDS
    | SHUTDOWN | SOURCES | SQLITE | STATISTICS | STDOUT | TOKEN | UNDROP | UNTIL | URL | VALID | WRITE
    | GRANTS | RECOMPRESS
    ;
alias: IDENTIFIER | keywordForAlias;  // |interval| can't be an alias, otherwise 'INTERVAL 1 SOMETHING' becomes ambiguous.
identifier: IDENTIFIER | interval | keyword;
identifierOrNull: identifier | NULL_SQL;  // NULL_SQL can be only 'Null' here.
enumValue: stringLiteral EQ_SINGLE numberLiteral;
