package com.simonellistonball.flink.demos;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class DateParser extends ScalarFunction {
    public long eval(String s) {
        String format = "yyyyMMddHHmmss." + new String(new char[s.length() - 14]).replace("\0", "S");
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern(format);
        return LocalDateTime.parse(s.replaceAll("(\\d{14})(\\d*)", "$1.$2" ), formatter).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public TypeInformation<?> getResultType(Class<?>[] signature) {
        return Types.SQL_TIMESTAMP;
    }
}
