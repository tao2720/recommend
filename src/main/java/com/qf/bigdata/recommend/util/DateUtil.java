package com.qf.bigdata.recommend.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;


/**
 * @Description: 之所以在Scala项目中使用一个Java编写一个工具类，是告诉你可以这样用，
 * 当你的业务程序之前有一些模型方法或工具方法不想用Scala重写一遍时，那就直接使用即可
 * @Author: QF
 * @Date: 2020/7/16 10:04 PM
 * @Version V1.0
 */

public class DateUtil {


    private static final String date_format = "yyyyMMdd";
    private static ThreadLocal<DateFormat> threadLocal = new ThreadLocal<DateFormat>();

    public static DateFormat getDateFormat()
    {
        DateFormat df = threadLocal.get();
        if(df==null){
            df = new SimpleDateFormat(date_format);
            threadLocal.set(df);
        }
        return df;
    }


    /**
     * 判断传入时间距离现在的天数
     * @param dataStr
     * @return
     * @throws ParseException
     */
    public static int diffDayToNow(String dataStr) throws ParseException {

        Calendar cal = Calendar.getInstance();
        cal.setTime(getDateFormat().parse(dataStr));
        long time1 = cal.getTimeInMillis();
        cal.setTime(new Date());
        long time2 = cal.getTimeInMillis();
        long between_days = (time2 - time1) / (1000 * 3600 * 24);

        return Integer.parseInt(String.valueOf(between_days));
    }

    /**
     * 获取指定范围内的所有日期字符串，包括开始和结束
     * @param startDateStr
     * @param endDateStr
     * @return
     * @throws ParseException
     */
    public static List<String> getRangeDateStr(String startDateStr, String endDateStr) throws ParseException {

        List<String> dateStrList=new ArrayList<String>();
        dateStrList.add(startDateStr);
        Calendar calStart=Calendar.getInstance();
        calStart.setTime(getDateFormat().parse(startDateStr));
        Calendar calEnd=Calendar.getInstance();
        calEnd.setTime(getDateFormat().parse(endDateStr));
        while (calEnd.after(calStart)){
            calStart.add(Calendar.DAY_OF_MONTH,1);
            dateStrList.add(getDateFormat().format(calStart.getTime()));
        }
        return dateStrList;
    }

}