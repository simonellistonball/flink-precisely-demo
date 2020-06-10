SELECT
    after_image.AddressID,
    after_image.AddressLine1,
    after_image.City,
    after_image.StateProvinceID,
    after_image.PostalCode,
    parseTs(after_image.ModifiedDate) as ModifiedDate,
    parseTs(sv_op_timestamp) as AddressAsOf
FROM cdc_Address