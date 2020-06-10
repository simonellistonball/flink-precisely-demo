INSERT INTO output
    SELECT
        TUMBLE_START(OrderAsOf, INTERVAL '10' MINUTE) as OrderPeriod,
        sum(TotalDue) as TotalDue,
        CountryRegionCode,
        StateName
    FROM ordersWithAddress
    GROUP BY
        CountryRegionCode,
        StateName,
        TUMBLE(OrderAsOf, INTERVAL '10' MINUTE)