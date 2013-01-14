package com.sohu.dc.jobkeeper.tools;

import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SendMsg {
	private static final Log logger = LogFactory.getLog(SendMsg.class);

	public static void main(String[] args) {
		SendMsg sendMsg = new SendMsg();
		sendMsg.sendMsg("15652299258", "test");
	}

	public void sendMsg(String mobile, String content) {

		Map<String, Object> map = new HashMap<String, Object>();
		map.put("key", "x1@9eng");
		map.put("src", mobile);
		map.put("fee", mobile);
		map.put("dest", mobile);
		map.put("mess", content);
		sendPost("http://ppt.sohu-inc.com/ppp/sns2.php", map);

	}

	/**
	 * 向指定URL发送POST方法的请求
	 * 
	 * @param requestUrl
	 *            请求的URL
	 * @param requestParamsMap
	 *            请求的参数
	 * @return response结果
	 * 
	 * @author yajun zhang
	 */

	private void sendPost(String requestUrl,
			Map<String, Object> requestParamsMap) {

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
			httpURLConnection.setRequestProperty("Content-Length", String
					.valueOf(params.length()));
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
}
