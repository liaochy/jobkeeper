package com.sohu.dc.jobkeeper.runtime;

public class ShellBean {
	private String name;

	public String getName() {
		return name;
	}

	public ShellBean(String name, String suffix, String date, String curHour) {
		super();
		this.name = name;
		this.suffix = suffix;
		this.date = date;
		this.curHour = curHour;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSuffix() {
		return suffix;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	private String suffix;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	private String date;
	private String curHour;

	public String getCurHour() {
		return curHour;
	}

	public void setCurHour(String curHour) {
		this.curHour = curHour;
	}

	
}
