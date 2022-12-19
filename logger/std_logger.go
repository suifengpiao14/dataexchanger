package logger

var logInfoChain = make(chan interface{})

//GetLoggerChain 获取日志接收通道
func GetLoggerChain() (readChain <-chan interface{}) {
	return logInfoChain
}

func SendLogInfo(label string, info interface{}) {
	select { // 不阻塞写入,避免影响主程序
	case logInfoChain <- info:
		return
	default:
		return
	}

}
