package com.sohu.dc.jobkeeper;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sohu.dc.zkutil.conf.Configuration;
import com.sohu.dc.zkutil.conf.Configured;

public class JobkeeperCommandLine extends Configured {
	private static final Log LOG = LogFactory
			.getLog(JobkeeperCommandLine.class);

	private static final String USAGE = "Usage: Jobkeeper [opts] jobkeeperId\n"
			+ " jobkeeperId  The Id to distinguish different Jobkeeper instances on a same machine\n"
			+ " where [opts] are:\n"
			+ "   --backup                  Master should start in backup mode";

	public JobkeeperCommandLine() {
		this.setConf(new Configuration());
	}

	public int doMain(String[] args) throws Exception {

		Options opt = new Options();
		opt.addOption("backup", false,
				"Do not try to become Jobkeeper until the primary fails");

		CommandLine cmd = null;
		try {
			cmd = new GnuParser().parse(opt, args);
		} catch (ParseException e) {
			LOG.error("Could not parse: ", e);
			System.out.println(USAGE);
			return -1;
		}

		// check if we are the backup master - override the conf if so
		if (cmd.hasOption("backup")) {
			getConf().setBoolean(JConstants.MASTER_BACKUP_KEY, true);
		}

		List<String> remainingArgs = cmd.getArgList();
		if (remainingArgs.size() != 1) {
			System.out.println(USAGE);
			return -1;
		}

		String jobkeeperId = remainingArgs.get(0);
		getConf().set(JConstants.MASTER_ID_KEY, jobkeeperId);
		return startJobkeeper();
	}

	private int startJobkeeper() {
		Configuration conf = getConf();
		try {
			Jobkeeper master = new Jobkeeper(conf);
			master.start();
		} catch (Throwable t) {
			LOG.error("Failed to start master", t);
			return -1;
		}
		return 0;
	}

}
