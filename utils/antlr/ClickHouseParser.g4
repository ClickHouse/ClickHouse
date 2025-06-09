parser grammar ClickHouseParser;

options {
    tokenVocab = ClickHouseLexer;
}

// Top-level statements

queryStmt
    : query (INTO OUTFILE stringLiteral)? (FORMAT identifierOrNull)? (SEMICOLON)?  # QueryStmtQuery
    | insertStmt                                                                   # QueryStmtInsert
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
    | selectUnionStmt
    | setStmt
    | showStmt
    | systemStmt
    | truncateStmt  // DDL
    | useStmt
    | watchStmt
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
    | (ATTACH | CREATE) LIVE VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? (WITH TIMEOUT DECIMAL_LITERAL?)? destinationClause? tableSchemaClause? subqueryClause   # CreateLiveViewStmt
    | (ATTACH | CREATE) MATERIALIZED VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? (destinationClause | engineClause POPULATE?) subqueryClause  # CreateMaterializedViewStmt
    | (ATTACH | CREATE (OR REPLACE)? | REPLACE) TEMPORARY? TABLE (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? engineClause? subqueryClause?                                 # CreateTableStmt
    | (ATTACH | CREATE) (OR REPLACE)? VIEW (IF NOT EXISTS)? tableIdentifier uuidClause? clusterClause? tableSchemaClause? subqueryClause                                              # CreateViewStmt
    ;

dictionarySchemaClause: LPAREN dictionaryAttrDfnt (COMMA dictionaryAttrDfnt)* RPAREN;
dictionaryAttrDfnt
locals [std::set<std::string> attrs]:
    identifier columnTypeExpr
    ( {!$attrs.count("default")}?      DEFAULT literal       {$attrs.insert("default");}
    | {!$attrs.count("expression")}?   EXPRESSION columnExpr {$attrs.insert("expression");}
    | {!$attrs.count("hierarchical")}? HIERARCHICAL          {$attrs.insert("hierarchical");}
    | {!$attrs.count("injective")}?    INJECTIVE             {$attrs.insert("injective");}
    | {!$attrs.count("is_object_id")}? IS_OBJECT_ID          {$attrs.insert("is_object_id");}
    )*
    ;
dictionaryEngineClause
locals [std::set<std::string> clauses]:
    dictionaryPrimaryKeyClause?
    ( {!$clauses.count("source")}? sourceClause {$clauses.insert("source");}
    | {!$clauses.count("lifetime")}? lifetimeClause {$clauses.insert("lifetime");}
    | {!$clauses.count("layout")}? layoutClause {$clauses.insert("layout");}
    | {!$clauses.count("range")}? rangeClause {$clauses.insert("range");}
    | {!$clauses.count("settings")}? dictionarySettingsClause {$clauses.insert("settings");}
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
destinationClause: TO tableIdentifier;
subqueryClause: AS selectUnionStmt;
tableSchemaClause
    : LPAREN tableElementExpr (COMMA tableElementExpr)* RPAREN  # SchemaDescriptionClause
    | AS tableIdentifier                                        # SchemaAsTableClause
    | AS tableFunctionExpr                                      # SchemaAsFunctionClause
    ;
engineClause
locals [std::set<std::string> clauses]:
    engineExpr
    ( {!$clauses.count("orderByClause")}?     orderByClause     {$clauses.insert("orderByClause");}
    | {!$clauses.count("partitionByClause")}? partitionByClause {$clauses.insert("partitionByClause");}
    | {!$clauses.count("primaryKeyClause")}?  primaryKeyClause  {$clauses.insert("primaryKeyClause");}
    | {!$clauses.count("sampleByClause")}?    sampleByClause    {$clauses.insert("sampleByClause");}
    | {!$clauses.count("ttlClause")}?         ttlClause         {$clauses.insert("ttlClause");}
    | {!$clauses.count("settingsClause")}?    settingsClause    {$clauses.insert("settingsClause");}
    )*
    ;
partitionByClause: PARTITION BY columnExpr;
primaryKeyClause: PRIMARY KEY columnExpr;
sampleByClause: SAMPLE BY columnExpr;
ttlClause: TTL ttlExpr (COMMA ttlExpr)*;

engineExpr: ENGINE EQ_SINGLE? identifierOrNull (LPAREN columnExprList? RPAREN)?;
tableElementExpr
    : tableColumnDfnt                                                              # TableElementExprColumn
    | CONSTRAINT identifier CHECK columnExpr                                       # TableElementExprConstraint
    | INDEX tableIndexDfnt                                                         # TableElementExprIndex
    | PROJECTION tableProjectionDfnt                                               # TableElementExprProjection
    ;
tableColumnDfnt
    : nestedIdentifier columnTypeExpr tableColumnPropertyExpr? (COMMENT stringLiteral)? codecExpr? (TTL columnExpr)?
    | nestedIdentifier columnTypeExpr? tableColumnPropertyExpr (COMMENT stringLiteral)? codecExpr? (TTL columnExpr)?
    ;
tableColumnPropertyExpr: (DEFAULT | MATERIALIZED | ALIAS) columnExpr;
tableIndexDfnt: nestedIdentifier columnExpr TYPE columnTypeExpr GRANULARITY DECIMAL_LITERAL;
tableProjectionDfnt: nestedIdentifier projectionSelectStmt;
codecExpr: CODEC LPAREN codecArgExpr (COMMA codecArgExpr)* RPAREN;
codecArgExpr: identifier (LPAREN columnExprList? RPAREN)?;
ttlExpr
    : columnExpr (DELETE whereClause? | TO DISK stringLiteral | TO VOLUME stringLiteral)?
    | columnExpr groupByClause SET ttlSetExpr (COMMA ttlSetExpr)*;
ttlSetExpr: columnExpr EQ_SINGLE columnExpr;

// DESCRIBE statement

describeStmt: (DESCRIBE | DESC) TABLE? tableExpr;

// DROP statement

dropStmt
    : (DETACH | DROP) DATABASE (IF EXISTS)? databaseIdentifier clusterClause?                                  # DropDatabaseStmt
    | (DETACH | DROP) (DICTIONARY | TEMPORARY? TABLE | VIEW) (IF EXISTS)? tableIdentifier clusterClause? (NO DELAY)?  # DropTableStmt
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

// INSERT statement

insertStmt: INSERT INTO TABLE? (tableIdentifier | FUNCTION tableFunctionExpr) columnsClause? dataClause;

columnsClause: LPAREN nestedIdentifier (COMMA nestedIdentifier)* RPAREN;
dataClause
    : FORMAT identifier                                                         # DataClauseFormat
    | VALUES  assignmentValues (COMMA assignmentValues)*                       # DataClauseValues
    | selectUnionStmt SEMICOLON? EOF                                            # DataClauseSelect
    ;

assignmentValues
    : LPAREN assignmentValue (COMMA assignmentValue)* RPAREN
    | LPAREN RPAREN
    ;
assignmentValue
    : literal
    ;

// KILL statement

killStmt
    : KILL MUTATION clusterClause? whereClause (SYNC | ASYNC | TEST)?  # KillMutationStmt
    ;

// OPTIMIZE statement

optimizeStmt: OPTIMIZE TABLE tableIdentifier clusterClause? partitionClause? FINAL? DEDUPLICATE?;

// RENAME statement

renameStmt: RENAME TABLE tableIdentifier TO tableIdentifier (COMMA tableIdentifier TO tableIdentifier)* clusterClause?;

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
selectStmtWithParens: selectStmt | LPAREN selectUnionStmt RPAREN;
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

// WATCH statement

watchStmt: WATCH tableIdentifier EVENTS? (LIMIT DECIMAL_LITERAL)?;



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
    | <assoc=right> columnExpr QUERY columnExpr COLON columnExpr                          # ColumnExprTernaryOp
    | columnExpr (alias | AS identifier)                                                  # ColumnExprAlias

    | (tableIdentifier DOT)? ASTERISK                                                     # ColumnExprAsterisk  // single-column only
    | LPAREN selectUnionStmt RPAREN                                                       # ColumnExprSubquery  // single-column only
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
    : tableIdentifier                    # TableExprIdentifier
    | tableFunctionExpr                  # TableExprFunction
    | LPAREN selectUnionStmt RPAREN      # TableExprSubquery
    | tableExpr (alias | AS identifier)  # TableExprAlias
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
    | DISTINCT | DISTRIBUTED | DROP | ELSE | ENABLED | END | ENGINE | ENGINES | ESTIMATE | EVENTS | EXCEPT | EXISTS | EXPLAIN | EXPRESSION | EXTENDED | EXTRACT | FETCHES | FIELDS | FILESYSTEM | FILL | FINAL | FIRST
    | FLUSH | FOLLOWING | FOR | FORMAT | FREEZE | FROM | FULL | FUNCTION | FUNCTIONS | GLOBAL | GRANULARITY | GRANTS | GROUP | GROUPING | HAVING | HIERARCHICAL | HOUR | ID
    | IF | ILIKE | IMPLICIT | IN | INDEX | INDEXES | INDICES | INJECTIVE | INNER | INSERT | INTERPOLATE | INTERVAL | INTO | IS | IS_OBJECT_ID | JOIN | JSON_FALSE | JSON_TRUE | KEY | KEYS
    | KILL | LAST | LAYOUT | LEADING | LEFT | LIFETIME | LIKE | LIMIT | LIVE | LOCAL | LOGS | MATERIALIZE | MATERIALIZED | MAX | MERGES
    | MICROSECOND | MILLISECOND | MIN | MINUTE | MODIFY | MOVE | MUTATION | NO | NOT | NULLS | OFFSET | ON | OPTIMIZE | OR | ORDER | OUTER | OUTFILE | OVER | OVERRIDE | PARTITION | PIPELINE | PLAN
    | POLICY | POLICIES | POPULATE | PRECEDING | PREWHERE | PRIMARY | PRIVILEGES | PROCESSLIST | PROFILE | PROFILES | PROJECTION | QUARTER | QUOTA | QUOTAS | RANGE | RECURSIVE | RELOAD | REMOVE | RENAME | REPLACE | REPLICA | REPLICATED | RIGHT | ROLE | ROLES | ROLLUP | ROW
    | ROWS | SAMPLE | SECOND | SELECT | SEMI | SENDS | SET | SETTING | SETTINGS | SHOW | SOURCE | START | STOP | SUBSTRING | SYNC | SYNTAX | SYSTEM | STEP | TABLE
    | TABLES | TEMPORARY | TEST | THEN | TIES | TIMEOUT | TIMESTAMP | TO | TOP | TOTALS | TRAILING | TREE | TRIM | TRUNCATE | TTL | TYPE
    | UNBOUNDED | UNION | UPDATE | USE | USER | USERS | USING | UUID | VALUES | VIEW | VOLUME | WATCH | WEEK | WHEN | WHERE | WINDOW | WITH | YEAR
    ;
keywordForAlias
    : AFTER | ALIAS | ALTER | AST | ASYNC | ATTACH | BOTH | CASE | CAST | CHECK | CLEAR | CLUSTER | CODEC
    | COLUMN | COMMENT | CONSTRAINT | CREATE | CUBE | CURRENT | DATABASE | DATABASES | DATE | DEDUPLICATE | DEFAULT | DELAY
    | DESCRIBE | DETACH | DICTIONARIES | DICTIONARY | DISK | DISTRIBUTED | DROP | ENGINE | EVENTS
    | EXISTS | EXPLAIN | EXPRESSION | EXTRACT | FETCHES | FLUSH | FOLLOWING | FREEZE | FUNCTION | GRANULARITY | HIERARCHICAL
    | ID | IF | INDEX | INJECTIVE | INSERT | IS_OBJECT_ID | KEY | KILL | LAYOUT | LEADING | LIFETIME | LIVE
    | LOGS | MATERIALIZE | MATERIALIZED | MAX | MERGES | MIN | MODIFY | MOVE | MUTATION | NO | OPTIMIZE | OUTFILE | OVER
    | PARTITION | POPULATE | PRECEDING | PRIMARY | RANGE | RELOAD | REMOVE | RENAME | REPLACE | REPLICA | REPLICATED | ROLLUP | ROW
    | SELECT | SENDS | SET | SHOW | SOURCE | START | STOP | SUBSTRING | SYNC | SYNTAX | SYSTEM | TABLE | TABLES | TEMPORARY
    | TEST | TIES | TIMEOUT | TIMESTAMP | TOTALS | TRAILING | TRIM | TRUNCATE | TTL | TYPE | UNBOUNDED | UPDATE
    | USE | UUID | VALUES | VIEW | VOLUME | WATCH
    ;
alias: IDENTIFIER | keywordForAlias;  // |interval| can't be an alias, otherwise 'INTERVAL 1 SOMETHING' becomes ambiguous.
identifier: IDENTIFIER | interval | keyword;
identifierOrNull: identifier | NULL_SQL;  // NULL_SQL can be only 'Null' here.
enumValue: stringLiteral EQ_SINGLE numberLiteral;
