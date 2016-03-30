package org.ngrinder.network;

import java.util.Set;

import net.grinder.console.communication.AgentProcessControlImplementation.AgentStatus;
import net.grinder.engine.controller.AgentControllerIdentityImplementation;
import net.grinder.util.UnitUtils;

import org.apache.commons.lang.StringUtils;
import org.ngrinder.extension.OnPeriodicWorkingAgentCheckRunnable;
import org.ngrinder.model.PerfTest;
import org.ngrinder.model.Status;
import org.ngrinder.service.IConfig;
import org.ngrinder.service.IPerfTestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import ro.fortsoft.pf4j.Extension;
import ro.fortsoft.pf4j.Plugin;
import ro.fortsoft.pf4j.PluginWrapper;

/**
 * Physical Network overflow blocking plugin. This plugin blocks tests which cause very large amount
 * of physical traffic.
 *
 * @author JunHo Yoon
 * @since 3.3
 */
public class PhysicalNetworkOverFlow extends Plugin {
	private static final Logger LOGGER = LoggerFactory.getLogger(PhysicalNetworkOverFlow.class);

	public PhysicalNetworkOverFlow(PluginWrapper wrapper) {
		super(wrapper);
	}

	@Extension
	public static class PhysicalNetworkOverFlowExtension implements
		OnPeriodicWorkingAgentCheckRunnable {
		private static final String PROP_NETWORK_OVERFLOW_LIMIT = "plugin.networkoverflow.limit";
		private static final int PROP_NETWORK_OVERFLOW_LIMIT_DEFAULT = 128;

		@Autowired
		private IConfig config;

		@Autowired
		private IPerfTestService perfTestService;

		public void checkWorkingAgent(Set<AgentStatus> workingAgents) {
			int totalReceived = 0;
			int totalSent = 0;
			if (workingAgents.isEmpty()) {
				return;
			}
			String region = "";
			for (AgentStatus each : workingAgents) {
				final String eachAgentRegion = ((AgentControllerIdentityImplementation) each.getAgentIdentity()).getRegion();
				if (!StringUtils.contains(eachAgentRegion, "owned_")) {
					region = eachAgentRegion;
					totalReceived += each.getSystemDataModel().getReceivedPerSec();
					totalSent += each.getSystemDataModel().getSentPerSec();
				}
			}

			int limit = config.getControllerProperties().getPropertyInt(
				PROP_NETWORK_OVERFLOW_LIMIT, PROP_NETWORK_OVERFLOW_LIMIT_DEFAULT) * 1024 * 1024;
			if (totalReceived > limit || totalSent > limit) {
				LOGGER.debug("LIMIT : {}, RX : {}, TX : {}", new Object[]{limit, totalReceived,
					totalSent});
				for (PerfTest perfTest : perfTestService.getAllTesting()) {
					if (perfTest.getStatus() != Status.ABNORMAL_TESTING) {
						perfTestService.markStatusAndProgress(
							perfTest,
							Status.ABNORMAL_TESTING,
							String.format("Too much traffic on the region %s. Stop by force.\n"
								+ "- LIMIT/s: %s\n" + "- RX/s: %s / TX/s: %s", region,
								UnitUtils.byteCountToDisplaySize(limit),
								UnitUtils.byteCountToDisplaySize(totalReceived),
								UnitUtils.byteCountToDisplaySize(totalSent)));
					}
				}
			}
		}
	}
}
