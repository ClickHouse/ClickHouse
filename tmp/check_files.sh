#!/bin/bash
set -e
FILES=(
  src/Analyzer/FunctionSecretArgumentsFinderTreeNode.cpp
  src/Analyzer/FunctionSecretArgumentsFinderTreeNode.h
  src/Analyzer/Resolve/resolveFunction.cpp
  src/Databases/DatabaseS3.cpp
  src/Databases/DatabaseS3.h
  src/Interpreters/ActionsDAG.cpp
  src/Interpreters/ActionsDAG.h
  src/Interpreters/InterpreterExplainQuery.cpp
  src/Parsers/ASTFunction.cpp
  src/Parsers/FunctionSecretArgumentsFinderAST.h
  src/Planner/PlannerActionsVisitor.cpp
  src/Processors/QueryPlan/QueryPlanFormat.cpp
  tests/queries/0_stateless/02968_url_args.reference
  tests/queries/0_stateless/03273_format_inference_create_query_s3_url.reference
  tests/queries/0_stateless/04343_secret_args_finder_mixed_named_positional.reference
  tests/queries/0_stateless/04510_s3_explicit_url_named_secret_mask.reference
  tests/queries/0_stateless/04510_s3_explicit_url_named_secret_mask.sql
  tests/queries/0_stateless/04628_secret_args_expression_derived_key.reference
  tests/queries/0_stateless/04628_secret_args_expression_derived_key.sql
  tests/queries/0_stateless/04648_url_secret_masking_forms.reference
  tests/queries/0_stateless/04648_url_secret_masking_forms.sql
)
for f in "${FILES[@]}"; do
  if git show 1e0acd6a91a0c82d20bad23f0e19785698289de7:"$f" > /tmp/master_ver_check 2>/dev/null; then
    if diff -q /tmp/master_ver_check "$f" > /dev/null 2>&1; then
      echo "MATCH: $f"
    else
      echo "DIFFERS: $f"
    fi
  else
    echo "MISSING IN MASTER?: $f"
  fi
done
