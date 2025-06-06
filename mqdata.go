package filemq

// MQData 队列内交互数据
type MQData interface {
	Type() MQDataType         // 返回数据类型名称
	Check() bool              // 检查数据是否允许对外输出
	Marshal() ([]byte, error) // 序列化数据
}

// DefaultMQData 默认队列数据结构
type DefaultMQData []byte

func (d DefaultMQData) Type() MQDataType {
	return DefaultMQFunc
}

func (d DefaultMQData) Check() bool {
	return true
}

func (d DefaultMQData) Marshal() ([]byte, error) {
	return d, nil
}

// DefaultUnmarshal 队列内交互数据反序列化
func DefaultUnmarshal(bytes []byte) (MQData, error) {
	return DefaultMQData(bytes), nil
}
