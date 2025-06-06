package filemq

type StringMQData string

func (s StringMQData) Type() MQDataType {
	return StringMQFunc
}

func (s StringMQData) Check() bool {
	return true
}

func (s StringMQData) Marshal() ([]byte, error) {
	return []byte(s), nil
}

func StringMQDataUnmarshal(bytes []byte) (MQData, error) {
	return StringMQData(bytes), nil
}
