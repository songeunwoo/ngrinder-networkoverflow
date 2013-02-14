package org.ngrinder.network;

import java.util.ArrayList;
import java.util.List;

import net.grinder.statistics.ImmutableStatisticsSet;
import net.grinder.statistics.StatisticsIndexMap;
import net.grinder.statistics.StatisticsIndexMap.LongIndex;

import org.apache.commons.lang.StringUtils;
import org.ngrinder.extension.OnTestSamplingRunnable;
import org.ngrinder.model.PerfTest;
import org.ngrinder.model.Status;
import org.ngrinder.service.IConfig;
import org.ngrinder.service.IPerfTestService;
import org.ngrinder.service.ISingleConsole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Network overflow plugin. This plugin blocks test running which causes the network overflow by the
 * large test.
 * 
 * @author JunHo Yoon
 * 
 */
public class NetworkOverFlow implements OnTestSamplingRunnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(NetworkOverFlow.class);
	private final IConfig config;
	private long limit;
	private List<String> allowedUser = new ArrayList<String>();

	public NetworkOverFlow(IConfig config) {
		this.config = config;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.ngrinder.extension.OnTestSamplingRunnable#startSampling(org.ngrinder.service.ISingleConsole
	 * , org.ngrinder.model.PerfTest, org.ngrinder.service.IPerfTestService)
	 */
	@Override
	public void startSampling(ISingleConsole singleConsole, PerfTest perfTest, IPerfTestService perfTestService) {
		limit = this.config.getSystemProperties().getPropertyInt("network.bandwidth.limit.megabyte", 128) * 1024 * 1024;
		for (String each : StringUtils.split(
						this.config.getSystemProperties().getProperty("network.bandwidth.allowed.user", ""), ',')) {
			allowedUser.add(StringUtils.trim(each));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.ngrinder.extension.OnTestSamplingRunnable#sampling(org.ngrinder.service.ISingleConsole,
	 * org.ngrinder.model.PerfTest, org.ngrinder.service.IPerfTestService,
	 * net.grinder.statistics.ImmutableStatisticsSet, net.grinder.statistics.ImmutableStatisticsSet)
	 */
	@Override
	public void sampling(ISingleConsole singleConsole, PerfTest perfTest, IPerfTestService perfTestService,
					ImmutableStatisticsSet intervalStatistics, ImmutableStatisticsSet cumulativeStatistics) {
		if (allowedUser.contains(perfTest.getCreatedUser().getUserId())) {
			return;
		}
		LongIndex longIndex = singleConsole.getStatisticsIndexMap().getLongIndex(
						StatisticsIndexMap.HTTP_PLUGIN_RESPONSE_LENGTH_KEY);
		Long byteSize = intervalStatistics.getValue(longIndex);

		if (byteSize > limit) {
			String message = String.format(
							"ERROR!! Network response over %d bytes per sec is not allowed due to the network capacity.\n"
											+ "%d bytes per sec were sent by this test.\n", limit, byteSize);
			LOGGER.info(message);
			LOGGER.info("Forcely Stop the test {}", perfTest.getTestIdentifier());
			perfTestService.markStatusAndProgress(perfTest, Status.ABNORMAL_TESTING, message);
			return;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.ngrinder.extension.OnTestSamplingRunnable#endSampling(org.ngrinder.service.ISingleConsole
	 * , org.ngrinder.model.PerfTest, org.ngrinder.service.IPerfTestService)
	 */
	@Override
	public void endSampling(ISingleConsole singleConsole, PerfTest perfTest, IPerfTestService perfTestService) {
		// TODO Auto-generated method stub

	}

}
