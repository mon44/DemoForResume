package com.ctrip.corp.settlement.airplus.business.common.entity.airplus;

import com.ctrip.corp.settlement.airplus.business.common.annotation.FieldDec;
import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author zwyu
 * @date 2020/07/13
 */
public class AirplusData {

    @SneakyThrows
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        List<Field> fields = Arrays.asList(this.getClass().getDeclaredFields())
                .stream().filter(f -> f.getAnnotation(FieldDec.class) != null).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(fields)) {
            return StringUtils.EMPTY;
        }
        fields.sort(Comparator.comparingInt(f -> f.getAnnotation(FieldDec.class).order()));
        for (Field field : fields) {
            field.setAccessible(true);
            if (Optional.ofNullable(field.getType().getSuperclass())
                    .map(p -> p.getName().equals(AirplusData.class.getName()))
                    .orElse(false)) {
                stringBuilder.append(field.get(this));
            } else if (field.getType() == List.class) {
                List list = (List) Optional.ofNullable(field.get(this)).orElse(Lists.newArrayList());
                list.stream().forEach(p -> stringBuilder.append(p));
            } else if (field.isAnnotationPresent(FieldDec.class)) {
                FieldDec fieldDec = field.getAnnotation(FieldDec.class);
                if (field.getType() == String.class) {
                    if (fieldDec.numbericFlag()) {
                        String value = Optional.ofNullable(field.get(this)).orElse("0").toString();
                        stringBuilder.append(StringUtils.leftPad(value, fieldDec.lenght(), "0"));
                    } else {
                        String value = Optional.ofNullable(field.get(this)).orElse(StringUtils.EMPTY).toString();
                        stringBuilder.append(limitStringLength(value, fieldDec.lenght()));
                    }
                } else if (field.getType() == Integer.class) {
                    String value = Optional.ofNullable(field.get(this)).orElse("0").toString();
                    stringBuilder.append(StringUtils.leftPad(value, fieldDec.lenght(), "0"));
                } else if (field.getType() == BigDecimal.class) {
                    BigDecimal value = (BigDecimal) Optional.ofNullable(field.get(this)).orElse(BigDecimal.ZERO);
                    DecimalFormat decimalFormat = new DecimalFormat("#0.00");
                    String strValue = decimalFormat.format(value).replace(".", StringUtils.EMPTY);
                    stringBuilder.append(StringUtils.leftPad(strValue, fieldDec.lenght(), "0"));
                }
            }
        }
        return stringBuilder.toString();
    }

    private static String limitStringLength(String fileCharString, int limitByteLen)
            throws UnsupportedEncodingException {
        String resultStr = StringUtils.EMPTY;
        int currentLen = 0;
        int charLen = fileCharString.getBytes("UTF-8").length;
        if (charLen > limitByteLen) {
            int reamingLen = 0;
            for (int i = 0; i < limitByteLen; i++) {
                reamingLen = limitByteLen - currentLen;
                if (currentLen % limitByteLen == currentLen) {
                    int charOneLen = String.valueOf(fileCharString.charAt(i)).getBytes("UTF-8").length;
                    if (charOneLen > reamingLen) {
                        resultStr = StringUtils.rightPad(resultStr, resultStr.length() + reamingLen, ' ');
                        break;
                    } else {
                        resultStr += String.valueOf(fileCharString.charAt(i));
                        currentLen += charOneLen;
                    }
                }
            }
        } else {
            resultStr = fileCharString;
            resultStr = StringUtils.rightPad(resultStr, resultStr.length() + limitByteLen - charLen, ' ');
        }
        return resultStr;
    }
}