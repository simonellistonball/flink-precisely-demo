SELECT
    after_image.StateProvinceID,
    after_image.CountryRegionCode,
    after_image.Name as StateName,
    parseTs(sv_op_timestamp) as StatesAsOf
FROM cdc_States