package config

const (
	LogDB = "logsDB" // 日志数据库

	LogColl        = "logs"        // 存储log的集合名称
	IPTotalColl    = "ip_total"    // 各个应用每天的独立ip数
	PVTotalColl    = "pv_total"    // 各个应用每天的pv数
	PVPareHourColl = "pv_pre_hour" // 每小时的pv数
	TCTOPUrlColl   = "tc_top_url"  // 平均耗时最高的url
)

var WorkDir string // 工作目录
