package filemq

import (
	"encoding/hex"
	"strings"
)

type MQDataType uint8 // 默认队列数据类型(0-255)

// HeadBytes 生成固定长度的数据头
func (mq MQDataType) HeadBytes() byte {
	return byte(mq)
}

const (
	DefaultMQFunc MQDataType = 200 + iota // 默认队列数据(也是默认内置类型的最小类型,自定义的类型最好不要定义大于这个值)
	TimeoutMQFunc                         // 超时队列数据
)

var (
	UnMarshalFuncMap = map[MQDataType]func([]byte) (MQData, error){
		DefaultMQFunc: DefaultUnmarshal,
		TimeoutMQFunc: TimeoutMQDataUnmarshal,
	}
)

// 将文件行数据切割成数据类型和数据
// 结构组成: 16进制字符串(数据类型 + 数据)
func cutFileLine(bytes []byte) (MQDataType, []byte) {
	// 去除末尾的换行符
	trimData := strings.TrimRight(string(bytes), "\n")
	fullData, err := hex.DecodeString(trimData)
	if err != nil || len(fullData) < 1 {
		return 0, nil
	}
	return MQDataType(fullData[0]), fullData[1:]
}

// 将数据组装成文件行数据
// 结构组成: 16进制字符串(数据类型 + 数据)
func makeFileLine(data MQData) ([]byte, error) {
	dataBytes, err := data.Marshal()
	if err != nil {
		return nil, err
	}

	// 构造完整二进制数据：数据类型 + 数据
	fullData := make([]byte, 1+len(dataBytes))
	fullData[0] = data.Type().HeadBytes()
	copy(fullData[1:], dataBytes)

	// 转换为十六进制字符串并返回字节切片
	hexStr := hex.EncodeToString(fullData) + "\n"
	return []byte(hexStr), nil
}
