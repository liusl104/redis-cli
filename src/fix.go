package redis_cli

func clusterManagerCommandFix(argc *Usage) {
	config.flags |= ClusterManagerCmdFlagFix
	clusterManagerCommandCheck(argc)
}
