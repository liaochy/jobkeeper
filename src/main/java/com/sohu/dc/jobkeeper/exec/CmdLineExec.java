package com.sohu.dc.jobkeeper.exec;

import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sohu.dc.jobkeeper.runtime.Node;

public class CmdLineExec implements CmdLineInterface {
	private final static Log logger = LogFactory.getLog(CmdLineExec.class);
	public final static String shellPath = CmdLineExec.class.getClassLoader()
			.getResource("").getPath().replace("classes/", "")
			+ "shell.sh";
	Node node;
	String timeArg;

	public CmdLineExec(Node node, String time) {
		super();
		this.node = node;
		this.timeArg = time;
	}

	public void exec() {
		final ExecProcesser proc = new ExecProcesser();
		proc.start();
	}

	private class ExecProcesser extends Thread {
		@Override
		public void run() {
			try {
				final DefaultExecutor executor = new DefaultExecutor();
				CommandLine cmd = new CommandLine(shellPath);
				String arguments = node.getArgument();
				// String type = node.getType();
				// String p_date = "";
				cmd.addArgument(node.getPath());
				cmd.addArgument(node.getOwner());
				// if ("day".equals(type)) {
				// p_date = DateUtil.getDateTime("yyyyMMdd", DateUtil.add(
				// new Date(), Calendar.DAY_OF_MONTH, -1));
				// } else {
				// p_date = DateUtil.getDateTime("yyyyMMddHH", DateUtil.add(
				// new Date(), Calendar.HOUR_OF_DAY, -1));
				// }
				if (arguments != null) {
					String[] argumentArray = arguments.split(",");
					for (String argument : argumentArray) {
						if ("-1".equals(argument)) {
							cmd.addArgument(timeArg);
							continue;
						}
						cmd.addArgument(argument);
					}
				}
				logger.info("executing scripts=" + cmd.toString());
				executor.execute(cmd);

			} catch (ExecuteException e) {
				logger.debug("ExecuteException=");
			} catch (IOException e) {
				logger.debug("IOException=");
			}
		}
	}
}
