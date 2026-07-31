DROP ROW POLICY IF EXISTS lance_local_prewhere_rls_policy ON lance_local_prewhere_rls;
DROP TABLE IF EXISTS lance_local_prewhere_rls;

CREATE TABLE lance_local_prewhere_rls
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/pushdown.lance');

CREATE ROW POLICY lance_local_prewhere_rls_policy
ON lance_local_prewhere_rls
FOR SELECT USING id IN (1, 3)
TO default;

SELECT throwIf(arraySort(groupArray(id)) != [1, 3])
FROM lance_local_prewhere_rls
FORMAT Null;

DROP ROW POLICY lance_local_prewhere_rls_policy ON lance_local_prewhere_rls;
DROP TABLE lance_local_prewhere_rls;
