package com.sohu.dc.jobkeeper.tools;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SendMsg {
	private static final Log logger = LogFactory.getLog(SendMsg.class);

	private static String[] urlArr;

	static {
		InputStream inputStream = SendMsg.class.getClassLoader().getResourceAsStream("sms-url.properties");
		Properties p = new Properties();
		try {
			p.load(inputStream);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		String urls = p.getProperty("sms.urls");
		String[] arr = urls.split(",");
		SendMsg.urlArr = arr;
	}

	public void sendMsg(String mobile, String content) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("mobile", mobile);
		try {
			map.put("content", URLEncoder.encode(content, "utf-8"));
		} catch (UnsupportedEncodingException e) {
			logger.error("send msg param error : " + e);
		}
		for (String url : SendMsg.urlArr) {
			try {
				sendGet(url, map);
				logger.error("request " + url + " succes! ");
				break;
			} catch (Exception e) {
				logger.error("request " + url + " error! \n" + e);
			}
		}
	}

	/**
	 * 向指定URL发送POST方法的请求
	 * 
	 * @param requestUrl
	 *            请求的URL
	 * @param requestParamsMap
	 *            请求的参数
	 */
	private void sendPost(String requestUrl, Map<String, Object> requestParamsMap) {

		PrintWriter printWriter = null;
		// BufferedReader bufferedReader = null;
		// StringBuffer responseResult = new StringBuffer();
		StringBuffer params = new StringBuffer();
		HttpURLConnection httpURLConnection = null;

		// 组织请求参数
		Iterator it = requestParamsMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry element = (Map.Entry) it.next();
			params.append(element.getKey());
			params.append("=");
			params.append(element.getValue());
			params.append("&");
		}
		if (params.length() > 0) {
			params.deleteCharAt(params.length() - 1);
		}

		try {
			URL realUrl = new URL(requestUrl);
			// 打开和URL之间的连接
			httpURLConnection = (HttpURLConnection) realUrl.openConnection();
			// 设置通用的请求属性
			httpURLConnection.setRequestProperty("accept", "*/*");
			httpURLConnection.setRequestProperty("connection", "Keep-Alive");
			httpURLConnection.setRequestProperty("Content-Length", String.valueOf(params.length()));
			// 发送POST请求必须设置如下两行
			httpURLConnection.setDoOutput(true);
			httpURLConnection.setDoInput(true);
			// 获取URLConnection对象对应的输出流
			printWriter = new PrintWriter(httpURLConnection.getOutputStream());
			// 发送请求参数
			printWriter.write(params.toString());
			// flush输出流的缓冲
			printWriter.flush();
			// 根据ResponseCode判断连接是否成功
			int responseCode = httpURLConnection.getResponseCode();
			if (responseCode != 200) {
				logger.error(" Error===" + responseCode);
				System.out.println(" Error===" + responseCode);
			}

		} catch (Exception e) {
			logger.error("send post request error!" + e);
		} finally {
			httpURLConnection.disconnect();
			if (printWriter != null) {
				printWriter.close();
			}
		}
	}

	/**
	 * 向指定URL发送GET方法的请求
	 * 
	 * @param requestUrl
	 *            请求的URL
	 * @param requestParamsMap
	 *            请求的参数
	 * @throws Exception
	 */

	private void sendGet(String requestUrl, Map<String, Object> requestParamsMap) throws Exception {
		// 组织请求参数
		StringBuffer params = new StringBuffer(requestUrl);
		params.append("?");
		Iterator it = requestParamsMap.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry element = (Map.Entry) it.next();
			params.append(element.getKey());
			params.append("=");
			params.append(element.getValue());
			params.append("&");
		}
		if (requestParamsMap.size() > 0) {
			params.deleteCharAt(params.length() - 1);
		}

		// 发送请求
		HttpURLConnection httpURLConnection = null;
		try {
			URL getUrl = new URL(params.toString());
			httpURLConnection = (HttpURLConnection) getUrl.openConnection();
			httpURLConnection.connect();
			httpURLConnection.getInputStream();
			int responseCode = httpURLConnection.getResponseCode();
			logger.info(params + " http status : " + responseCode);
			if (responseCode != 200) {
				throw new Exception();
			}
		} catch (Exception e) {
			throw new Exception();
		} finally {
			if (httpURLConnection != null) {
				httpURLConnection.disconnect();
			}
		}

	}

	public static void main(String[] args) {
		SendMsg sendMsg = new SendMsg();
		sendMsg.sendMsg("18633898992", "中文 8090");
	}

}
