package com.sohu.dc.jobkeeper.tools;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Date Utility Class This is used to convert Strings to Dates and Timestamps
 * 
 * <p>
 * <a href="DateUtil.java.html"><i>View Source</i></a>
 * </p>
 * 
 * @author yajunzhang
 * @version $Revision: 1.1 $ $Date: 2008/03/04 02:15:55 $
 */
public class DateUtil {
	// ~ Static fields/initializers
	// =============================================

	private static Log log = LogFactory.getLog(DateUtil.class);
	private static String defaultDatePattern = "yyyy-MM-dd";
	private static String timePattern = "HH:mm";

	// ~ Methods
	// ================================================================
	/**
	 * 获取年份
	 * 
	 * @param calendar
	 * @return
	 */
	public static int getYear(Calendar calendar) {
		return calendar.get(Calendar.YEAR);
	}

	/**
	 * 获取季度
	 * 
	 * @param calendar
	 * @return
	 */
	public static int getQuarter(Calendar calendar) {

		int nQuarter = 0;
		int nMonth = getMonth(calendar);

		if (nMonth >= 1 && nMonth <= 3) {
			nQuarter = 1;
		} else if (nMonth >= 4 && nMonth <= 6) {
			nQuarter = 2;
		} else if (nMonth >= 7 && nMonth <= 9) {
			nQuarter = 3;
		} else if (nMonth >= 10 && nMonth <= 12) {
			nQuarter = 4;
		}

		return nQuarter;
	}

	/**
	 * 获取月份
	 * 
	 * @param calendar
	 * @return
	 */
	public static int getMonth(Calendar calendar) {
		return calendar.get(Calendar.MONTH) + 1;
	}

	/**
	 * 获取周
	 * 
	 * @param calendar
	 * @return
	 */
	public static int getWeekOfYear(Calendar calendar) {
		return calendar.get(Calendar.WEEK_OF_YEAR);
	}

	/**
	 * 获取日
	 * 
	 * @param calendar
	 * @return
	 */
	public static int getDay(Calendar calendar) {
		return calendar.get(Calendar.DAY_OF_MONTH);
	}

	/**
	 * 获取星期几
	 * 
	 * @param calendar
	 * @return
	 */
	public static int getDayOfWeek(Calendar calendar) {
		int i = 0;
		i = calendar.get(Calendar.DAY_OF_WEEK);
		if (i > 1)
			i--;
		else if (i == 1)
			i = 7;

		return i;
	}

	// 以上面向Calendar的接口
	// ================================================================

	/**
	 * Return default datePattern (yyyy-MM-dd)
	 * 
	 * @return a string representing the date pattern on the UI
	 */
	public static String getDatePattern() {
		return defaultDatePattern;
	}

	public static String getDateTimePattern() {
		return DateUtil.getDatePattern() + " HH:mm:ss.S";
	}

	/**
	 * This method attempts to convert an Mysql-formatted date in the form
	 * yyyy-MMM-dd HH:mm:ss.S to yyyy-mm-dd.
	 * 
	 * @param aDate
	 *            date from database as a string
	 * @return formatted string for the ui
	 */
	public static final String getDate(Date aDate) {
		SimpleDateFormat df = null;
		String returnValue = "";

		if (aDate != null) {
			df = new SimpleDateFormat(getDatePattern());
			returnValue = df.format(aDate);
		}

		return (returnValue);
	}

	/**
	 * This method generates a string representation of a date/time in the
	 * format you specify on input
	 * 
	 * @param aMask
	 *            the date pattern the string is in
	 * @param strDate
	 *            a string representation of a date
	 * @return a converted Date object
	 * @see java.text.SimpleDateFormat
	 * @throws ParseException
	 */
	public static final Date convertStringToDate(String aMask, String strDate)
			throws ParseException {
		SimpleDateFormat df = null;
		Date date = null;
		df = new SimpleDateFormat(aMask);

		if (log.isDebugEnabled()) {
			log.debug("converting '" + strDate + "' to date with mask '"
					+ aMask + "'");
		}

		try {
			date = df.parse(strDate);
		} catch (ParseException pe) {
			log.error("ParseException: " + pe);
			throw new ParseException(pe.getMessage(), pe.getErrorOffset());
		}

		return (date);
	}

	/**
	 * This method returns the current date time in the format:yyyy-MM-dd HH:MM
	 * a
	 * 
	 * @param theTime
	 *            the current time
	 * @return the current date/time
	 */
	public static String getTimeNow(Date theTime) {
		return getDateTime(timePattern, theTime);
	}

	/**
	 * This method returns the current date in the format: yyyy-MM-dd
	 * 
	 * @return the current date
	 * @throws ParseException
	 */
	public static Calendar getToday() throws ParseException {
		Date today = new Date();
		SimpleDateFormat df = new SimpleDateFormat(getDatePattern());

		// This seems like quite a hack (date -> string -> date),
		// but it works ;-)
		String todayAsString = df.format(today);
		Calendar cal = new GregorianCalendar();
		cal.setTime(convertStringToDate(todayAsString));

		return cal;
	}

	/**
	 * This method generates a string representation of a date's date/time in
	 * the format you specify on input
	 * 
	 * @param aMask
	 *            the date pattern the string is in
	 * @param aDate
	 *            a date object
	 * @return a formatted string representation of the date
	 * 
	 * @see java.text.SimpleDateFormat
	 */
	public static final String getDateTime(String aMask, Date aDate) {
		SimpleDateFormat df = null;
		String returnValue = "";

		if (aDate == null) {
			log.error("aDate is null!");
		} else {
			df = new SimpleDateFormat(aMask);
			returnValue = df.format(aDate);
		}

		return (returnValue);
	}

	/**
	 * This method generates a string representation of a date based on the
	 * System Property 'dateFormat' in the format you specify on input
	 * 
	 * @param aDate
	 *            A date to convert
	 * @return a string representation of the date
	 */
	public static final String convertDateToString(Date aDate) {
		return getDateTime(getDatePattern(), aDate);
	}

	/**
	 * This method converts a String to a date using the datePattern
	 * 
	 * @param strDate
	 *            the date to convert (in format yyyy-MM-dd)
	 * @return a date object
	 * 
	 * @throws ParseException
	 */
	public static Date convertStringToDate(String strDate)
			throws ParseException {
		Date aDate = null;

		try {
			if (log.isDebugEnabled()) {
				log.debug("converting date with pattern: " + getDatePattern());
			}

			aDate = convertStringToDate(getDatePattern(), strDate);
		} catch (ParseException pe) {
			log.error("Could not convert '" + strDate
					+ "' to a date, throwing exception");
			pe.printStackTrace();
			throw new ParseException(pe.getMessage(), pe.getErrorOffset());

		}

		return aDate;
	}

	/**
	 * This method generates a long representation of a date based on the System
	 * Property 'dateFormat' in the format you specify on input
	 * 
	 * @param aDate
	 *            A date to convert
	 * @return a long representation of the date
	 */
	public static final Long convertDateToLong(Date aDate) {
		return aDate.getTime();
	}

	/**
	 * This method converts a Long to a date using the datePattern
	 * 
	 * @param longDate
	 *            the date to convert (in format mills)
	 * @return a date object
	 * 
	 * @throws ParseException
	 */
	public static Date convertLongToDate(Long longDate) throws Exception {
		Date aDate = null;

		try {
			if (log.isDebugEnabled()) {
				log.debug("converting date with pattern: " + getDatePattern());
			}
			aDate = new Date(longDate);
		} catch (Exception pe) {
			log.error("Could not convert '" + longDate
					+ "' to a date, throwing exception");
			pe.printStackTrace();
			throw new Exception(pe.getMessage());

		}

		return aDate;
	}

	/**
	 * 取出固定格式的时间.
	 * 
	 * @param year
	 * @param month
	 * @param date
	 * @param format
	 * @return
	 */
	public static String getFormatTime(Integer year, Integer month,
			Integer date, String formats) {
		GregorianCalendar calendar = new GregorianCalendar(year, month, date);
		Date da = calendar.getTime();
		SimpleDateFormat format = new SimpleDateFormat(formats);
		return format.format(da);
	}

	/**
	 * 
	 * @param aDate
	 * @param formats
	 * @return .
	 */
	public static String getFormatTime(Date aDate, String formats) {
		SimpleDateFormat format = new SimpleDateFormat(formats);
		return format.format(aDate);
	}

	/**
	 * 根据阴历年份计算生肖 .
	 * 
	 * @param year
	 *            .
	 * @return .
	 */
	public static String getAnimalByYear(Integer year) {
		String animal = "";
		if (year == null || year <= 0)
			return animal;
		int start = 1901;
		int x = (start - year) % 12;
		if (x == 1 || x == -11) {
			animal = "鼠";
		}
		if (x == 0) {
			animal = "牛";
		}
		if (x == 11 || x == -1) {
			animal = "虎";
		}
		if (x == 10 || x == -2) {
			animal = "兔";
		}
		if (x == 9 || x == -3) {
			animal = "龙";
		}
		if (x == 8 || x == -4) {
			animal = "蛇";
		}
		if (x == 7 || x == -5) {
			animal = "马";
		}
		if (x == 6 || x == -6) {
			animal = "羊";
		}
		if (x == 5 || x == -7) {
			animal = "猴";
		}
		if (x == 4 || x == -8) {
			animal = "鸡";
		}
		if (x == 3 || x == -9) {
			animal = "狗";
		}
		if (x == 2 || x == -10) {
			animal = "猪";
		}
		return animal;
	}

	/**
	 * 根据阳历日期计算星座 .
	 * 
	 * @param month
	 *            .
	 * @param date
	 *            .
	 * @return .
	 */
	public static String getStarByDate(Integer month, Integer date) {

		String star = "";
		if (month == null || date == null || month <= 0 || date <= 0)
			return star;

		if (month == 1 && date >= 20 || month == 2 && date <= 18) {
			star = "水瓶座";
		}

		if (month == 2 && date >= 19 || month == 3 && date <= 20) {
			star = "双鱼座";
		}

		if (month == 3 && date >= 21 || month == 4 && date <= 19) {
			star = "白羊座";
		}

		if (month == 4 && date >= 20 || month == 5 && date <= 20) {
			star = "金牛座";
		}

		if (month == 5 && date >= 21 || month == 6 && date <= 21) {
			star = "双子座";
		}

		if (month == 6 && date >= 22 || month == 7 && date <= 22) {
			star = "巨蟹座";
		}

		if (month == 7 && date >= 23 || month == 8 && date <= 22) {
			star = "狮子座";
		}

		if (month == 8 && date >= 23 || month == 9 && date <= 22) {
			star = "处女座";
		}

		if (month == 9 && date >= 23 || month == 10 && date <= 22) {
			star = "天秤座";
		}

		if (month == 10 && date >= 23 || month == 11 && date <= 21) {
			star = "天蝎座";
		}

		if (month == 11 && date >= 22 || month == 12 && date <= 21) {
			star = "射手座";
		}

		if (month == 12 && date >= 22 || month == 1 && date <= 19) {
			star = "摩羯座";
		}

		return star;

	}

	/**
	 * 
	 * @param date
	 * @return
	 */
	public static String dayOfWeek(Date date) {

		String dayOfWeek = "";

		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int day = c.get(Calendar.DAY_OF_WEEK);

		switch (day) {
		case 1:
			dayOfWeek = "星期天";
			break;
		case 2:
			dayOfWeek = "星期一";
			break;
		case 3:
			dayOfWeek = "星期二";
			break;
		case 4:
			dayOfWeek = "星期三";
			break;
		case 5:
			dayOfWeek = "星期四";
			break;
		case 6:
			dayOfWeek = "星期五";
			break;
		case 7:
			dayOfWeek = "星期六";
			break;
		}

		return dayOfWeek;

	}

	/**
	 * 根据年份计算年纪 .
	 * 
	 * @param year
	 *            .
	 * @return .
	 */
	public static Integer age(int year) {
		int age = 0;
		Date date = new Date();
		SimpleDateFormat sd = new SimpleDateFormat("yyyy");
		age = Integer.parseInt(sd.format(date)) - year;
		return age;

	}

	/**
	 * 将date转换成形如20101112的整数
	 * 
	 * @param date
	 * @return
	 */
	public static int convertDate2Timeid(Date date) {
		int timeid = 0;
		String strDate = getDateTime("yyyyMMdd", date);
		if (strDate != null && strDate.length() > 0) {
			timeid = Integer.parseInt(strDate);
		}
		return timeid;
	}

	/**
	 * 将小时转换成形如2010111201的整数
	 * 
	 * @param date
	 * @param hour
	 * @return
	 */
	public static int convertDateHour2Timeid(Date date, int hour) {
		int timeid = 0;
		timeid = convertDate2Timeid(date);
		timeid = timeid * 100 + hour;
		return timeid;
	}

	/**
	 * 获取当前小时整数
	 * 
	 * @param date
	 * @return
	 */
	public static int getHour(Date date) {
		int hour = 0;
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		hour = c.get(Calendar.HOUR_OF_DAY);
		return hour;
	}

	/**
	 * 日期的加减
	 * 
	 * @param date
	 * @param distance
	 * @return
	 */
	public static Date getDateByDistance(Date date, int offset) {
		Date theday = null;
		long theTime = (date.getTime() / 1000) + 60 * 60 * 24 * offset;
		theday = new Date(theTime * 1000);
		return theday;
	}

	/**
	 * 得到前一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getPreDay(Date date) {
		return getDateByDistance(date, -1);
	}

	/**
	 * 得到后一天
	 * 
	 * @param date
	 * @return
	 */
	public static Date getNextDay(Date date) {
		return getDateByDistance(date, 1);
	}

	/**
	 * Adds or subtracts the specified amount of time to the given date field,
	 * based on the calendar's rules. For example, to subtract 5 days from the
	 * current date, you can achieve it by calling:
	 * <p>
	 * <code>add(new Date(),Calendar.DAY_OF_MONTH, -5)</code>.
	 * 
	 * @param date
	 * @param calendarField
	 *            the calendar field.
	 * @param amount
	 *            the amount of date or time to be added to the field.
	 * 
	 */
	public static Date add(Date date, int calendarField, int amount) {
		if (date == null) {
			throw new IllegalArgumentException("The date must not be null");
		}
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		c.add(calendarField, amount);
		return c.getTime();
	}

	public static int getPerHour(int hour) {
		int thehour = 0;

		if (hour == 23)
			thehour = 0;
		else
			thehour = hour + 1;
		return thehour;
	}

	public static int getNextHour(int hour) {
		int thehour = 0;
		if (hour == 0)
			thehour = 23;
		else
			thehour = hour - 1;
		return thehour;
	}
	
	
	public static int getPeriod(int timeId,int offset)throws ParseException{
		int  retId = timeId;
		Date date = null;
		if(timeId > 1000000000){  //小时
			int hour = timeId % 100;
			
		}
		else{ //日
			date = convertStringToDate("yyyyMMdd", ""+timeId);	
			date = getDateByDistance(date, offset);
			retId = DateUtil.convertDate2Timeid(date);
		}
		
		return retId;
	}


	public static void main(String[] args) {
		System.out.println(dayOfWeek(new Date()));
	}

}
