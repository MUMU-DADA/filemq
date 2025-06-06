package filemq

import (
	"os"
	"testing"
	"time"
)

func TestTimeoutDataCheck(t *testing.T) {
	tempFile := "test_timeout.filemq"

	mq, err := New(tempFile, 10, map[MQDataType]func([]byte) (MQData, error){
		TimeoutMQFunc: TimeoutMQDataUnmarshal,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()

	timeoutData := &TimeoutMQData{
		Timeout: now.Add(1 * time.Second),
		Data:    []byte("timeout message 1"),
	}

	// 写入数据
	err = mq.Write(timeoutData)
	if err != nil {
		t.Fatal(err)
	}

	// 尝试读取
	data, err := mq.Read()
	if time.Now().Sub(now).Seconds() < 1 {
		t.Fatal("read timeout message error")
	}
	if string(data.(*TimeoutMQData).Data) != "timeout message 1" {
		t.Errorf("expected timeout message 1, got %v", string(data.(*TimeoutMQData).Data))
	}

	// 正常消息应能读出
	validData := &TimeoutMQData{
		Timeout: time.Now().Add(-time.Hour),
		Data:    []byte("timeout message 2"),
	}
	mq.Write(validData)

	msg, err := mq.Read()
	if err != nil {
		t.Fatal(err)
	}

	if string(msg.(*TimeoutMQData).Data) != "timeout message 2" {
		t.Errorf("expected timeout message 2, got %v", string(msg.(*TimeoutMQData).Data))
	}

	mq.Close()
	os.Remove(tempFile)
}

func TestBasicReadWrite(t *testing.T) {
	tempFile := "test_basic.filemq"

	mq, err := New(tempFile, 10, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 写入数据
	data := DefaultMQData([]byte("hello world"))
	err = mq.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	// 读取数据
	readData, err := mq.Read()
	if err != nil {
		t.Fatal(err)
	}

	// 验证内容
	if string(readData.(DefaultMQData)) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", readData.(DefaultMQData))
	}

	// 关闭并验证剩余数据
	left := mq.Close()

	if len(left) != 0 {
		t.Errorf("expected 0 remaining data after close, got %d", len(left))
	}

	os.Remove(tempFile)
}
func TestFileMQ_Basic(t *testing.T) {
	tempFile := "test_basic.filemq"

	// 创建 FileMQ 实例
	mq, err := New(tempFile, 10, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 写入数据
	data := DefaultMQData([]byte("hello world"))
	err = mq.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	// 读取数据
	readData, err := mq.Read()
	if err != nil {
		t.Fatal(err)
	}

	// 检查内容是否一致
	if string(readData.(DefaultMQData)) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", readData.(DefaultMQData))
	}

	// 关闭并获取剩余数据
	left := mq.Close()

	_ = left

	// 验证关闭后是否还能读写
	if _, err := mq.Read(); err == nil {
		t.Error("expected error when reading after close")
	}
	if err := mq.Write(DefaultMQData([]byte("after close"))); err == nil {
		t.Error("expected error when writing after close")
	}

	// 清理
	os.Remove(tempFile)
}

func BenchmarkFileMQ_WriteRead(b *testing.B) {
	tempFile := "benchmark.filemq"

	// 初始化
	mq, _ := New(tempFile, 0, nil)
	defer func() {
		mq.Close()
		os.Remove(tempFile)
	}()

	// 并行测试
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := DefaultMQData([]byte("benchmark data"))
		_ = mq.Write(msg)
		_, _ = mq.Read()
	}
}

func BenchmarkWriteOnly(b *testing.B) {
	tempFile := "benchmark_writeonly.filemq"

	mq, _ := New(tempFile, b.N, nil)
	defer func() {
		mq.Close()
		os.Remove(tempFile)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := DefaultMQData([]byte("benchmark data"))
		_ = mq.Write(msg)
	}
}

func BenchmarkReadFromDisk(b *testing.B) {
	tempFile := "benchmark_readfromdisk.filemq"

	// 初始化一些数据
	mq, _ := New(tempFile, b.N, nil)
	for i := 0; i < 1000; i++ {
		_ = mq.Write(DefaultMQData([]byte("benchmark data")))
	}
	mq.Close()

	// 重新打开进行读取测试
	mq, _ = New(tempFile, b.N, nil)
	defer func() {
		mq.Close()
		os.Remove(tempFile)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := mq.Read()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMixedReadWrite(b *testing.B) {
	tempFile := "benchmark_mixed.filemq"

	mq, _ := New(tempFile, b.N, nil)
	defer func() {
		mq.Close()
		os.Remove(tempFile)
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := DefaultMQData([]byte("mixed data"))
		_ = mq.Write(msg)
		_, _ = mq.Read()
	}
}
