DELETE FROM ta.factresult target
USING (
	SELECT 
	id,
	ssorid,
	ROW_NUMBER () OVER (PARTITION BY ssorid, testrunid ORDER BY executionenddate desc) AS rownum
	FROM ta.factresult
) count
WHERE count.id = target.id AND count.rownum > 1;
DELETE FROM ta.dimlatestresult target
USING (
	SELECT 
	id,
	ssorid,
	ROW_NUMBER () OVER (PARTITION BY ssorid, testrunid ORDER BY executionenddate desc) AS rownum
	FROM ta.dimlatestresult
) count
WHERE count.id = target.id AND count.rownum > 1;