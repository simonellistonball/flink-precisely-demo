SELECT
    after_image.SalesOrderId,
    parseTs(after_image.OrderDate) as OrderDate,
    parseTs(after_image.DueDate) as DueDate,
    parseTs(after_image.ShipDate) as ShipDate,
    after_image.Status,
    after_image.ShipToAddressId as AddressId,
    after_image.SubTotal,
    after_image.TaxAmt,
    after_image.Freight,
    after_image.TotalDue,
    parseTs(sv_op_timestamp) as OrderAsOf
FROM cdc_Orders