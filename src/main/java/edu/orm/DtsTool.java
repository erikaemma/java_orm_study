package edu.orm;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public abstract class DtsTool {
	
	/**
     * Java将Unix时间戳转换成指定格式日期字符串
     *
     * @param timestampString 时间戳 如："1473048265";
     * @param formats 要格式化的格式 默认："yyyy-MM-dd HH:mm:ss";
     *
     * @return 返回结果 如："2016-09-05 16:06:42";
     */
	public static String TimeStamp2Date(String timestampString, String formats) {
		if (formats == null || formats.isEmpty())
			formats = "yyyy-MM-dd HH:mm:ss";
		Long timestamp = Long.parseLong(timestampString) * 1000;
		String date = new SimpleDateFormat(formats, Locale.CHINA).format(new Date(timestamp));
		return date;
	}
	
	/**
     * 日期格式字符串转换成时间戳
     *
     * @param dateStr 字符串日期
     * @param format   如：yyyy-MM-dd HH:mm:ss
     *
     * @return
     */
	public static String Date2TimeStamp(String dateStr, String format) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return String.valueOf(sdf.parse(dateStr).getTime() / 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }
	
	/**
	 * 
	 * 从ResultSet结果集中获取Datetime列的字符串数值
	 * 要求数据库连接使用useTimezone=true参数
	 * 
	 * @param rs 结果集
	 * @param columnName 列名
	 * @param _timezone 时区
	 * @return
	 */
	public static String getDatetime(ResultSet rs, String columnName, String _timezone) {
		if(rs == null || columnName == null || _timezone == null || columnName.isEmpty() || _timezone.isEmpty())
			return null;
		try {
			Timestamp ts = rs.getTimestamp(columnName, Calendar.getInstance(TimeZone.getTimeZone(_timezone)));
			String tss = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
			return tss;
		} catch(SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}
	
	/**
	 * 
	 * getDatetime重载方式，默认北京时间
	 * 
	 * @param rs 结果集
	 * @param columnNmae 列名
	 * @return
	 */
	public static String getDatetime(ResultSet rs, String columnNmae) {
		return DtsTool.getDatetime(rs, columnNmae, "GMT+8");
	}
	
	/**
	 * 获取当前时间的yyyy-MM-dd HH:mm:ss格式字符串
	 * 
	 * @return
	 */
	public static String getNowDatetime() {
		Date date = new Date();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return format.format(date);
	}
}
