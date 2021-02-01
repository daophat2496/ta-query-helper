SELECT 1
FROM (
	SELECT id
		,ssorid
		,ROW_NUMBER() OVER (
			PARTITION BY ssorid
			,testrunid ORDER BY executionenddate DESC
			) AS rownum
	FROM ta.factresult
	) count
WHERE count.rownum > 1
LIMIT 1;