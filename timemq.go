package filemq

import (
	"encoding/json"
	"time"
)

// TimeoutMQData 队列内交互数据
type TimeoutMQData struct {
	Timeout time.Time
	Data    []byte
}

func (mq *TimeoutMQData) Type() MQDataType {
	return TimeoutMQFunc
}

func (mq *TimeoutMQData) Marshal() ([]byte, error) {
	return json.Marshal(mq)
}

// TimeoutMQDataUnmarshal 队列内交互数据反序列化
func TimeoutMQDataUnmarshal(bytes []byte) (MQData, error) {
	var temp TimeoutMQData
	err := json.Unmarshal(bytes, &temp)
	if err != nil {
		return nil, err
	}
	return &temp, nil
}

// Check 检查数据是否超时
func (mq *TimeoutMQData) Check() bool {
	return time.Now().After(mq.Timeout)
}
