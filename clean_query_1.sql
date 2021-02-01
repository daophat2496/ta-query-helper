UPDATE ta.DimTestCase target
SET
	roweffectivedate = DATE(TO_TIMESTAMP(1318673233000 / 1000) AT TIME ZONE 'UTC')
	, rowexpirationdate = NULL
	, iscurrent = True
FROM (
	SELECT 
		id
		, roweffectivedate
		, row_number() over (partition by ssor, ssorid order by roweffectivedate, id DESC NULLS LAST) as row_num
	FROM ta.DimTestCase dtc
) tmp
WHERE tmp.row_num = 1
	AND tmp.roweffectivedate IS NULL
	AND tmp.id = target.id;

DELETE FROM ta.DimTestCase
WHERE roweffectivedate IS NULL;