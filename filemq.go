package filemq

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var (
	initMQMap = sync.Map{} // 初始化的队列数据 用于防止监听同名文件的队列
)

const (
	Buffer_SIZE            = 1024                   // 缓存大小
	DEFAULT_TICK           = time.Millisecond * 100 // 清理间隔
	DEFAULT_CLEAN_LINE_NUM = 100                    // 开始清理行数
)

// FileMQ 文件队列
type FileMQ struct {
	filePath         string                                      // 文件名
	readBuffer       chan MQData                                 // 读取缓存
	writeBuffer      chan MQData                                 // 写入缓存
	bufferSize       int                                         // chan缓存大小
	unmarshalFuncMap map[MQDataType]func([]byte) (MQData, error) // 反序列化方法
	cleanLineMap     map[int]struct{}                            // 清理行号
	lock             sync.Mutex                                  // 锁
	stop             bool                                        // 停止信号
	wg               sync.WaitGroup                              // 停止协程辅助wg
}

// New 创建文件队列
func New(filePath string, bufferSize int, unmarshalFuncMap map[MQDataType]func([]byte) (MQData, error)) (*FileMQ, error) {
	if bufferSize == 0 {
		bufferSize = Buffer_SIZE
	}

	temp := &FileMQ{
		filePath:         filePath,
		readBuffer:       make(chan MQData, bufferSize),
		writeBuffer:      make(chan MQData, bufferSize),
		bufferSize:       bufferSize,
		unmarshalFuncMap: map[MQDataType]func([]byte) (MQData, error){},
		cleanLineMap:     map[int]struct{}{},
	}

	// 首先载入默认的数据
	for k, v := range UnMarshalFuncMap {
		temp.unmarshalFuncMap[k] = v
	}
	// 载入用户自定义的解析方法
	for k, v := range unmarshalFuncMap {
		temp.unmarshalFuncMap[k] = v
	}

	// 初始化文件队列文件
	if err := temp.init(); err != nil {
		return nil, err
	}

	go temp.goRead()  // 启动读取协程
	go temp.goWrite() // 启动写入协程
	go temp.goClean() // 启动清理协程

	return temp, nil
}

// Close 关闭文件队列
func (f *FileMQ) Close() []MQData {
	f.wg = sync.WaitGroup{} // 重置
	f.wg.Add(3)             // 3个协程
	f.stop = true           // 停止信号
	f.wg.Wait()             // 等待三个协程关闭

	// 读取在读缓存中未输出完成的数据
	outdata := []MQData{}

	// 处理读缓存
	close(f.readBuffer)
	for msg := range f.readBuffer {
		outdata = append(outdata, msg)
	}

	// 处理写缓存
	close(f.writeBuffer)
	for msg := range f.writeBuffer {
		// 检查缓存是否可以提取
		if msg.Check() {
			outdata = append(outdata, msg) // 可以提取的数据直接提取输出
		} else {
			err := f.writeMQData(msg) // 不可以提取的数据写入文件
			if err != nil {
				slog.Error("writeMQData error", "err", err)
			}
		}
	}

	// 在关闭的最后移除注册
	absPath, _ := filepath.Abs(f.filePath)
	initMQMap.Delete(absPath)

	return outdata
}

// AddUnmarshalFunc 添加反序列化函数 (用于工作时需要额外处理新类型时添加额外的类型解析函数)
func (f *FileMQ) AddUnmarshalFunc(mqType MQDataType, unmarshalFunc func([]byte) (MQData, error)) {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.unmarshalFuncMap[mqType] = unmarshalFunc
}

// 读取文件队列
func (f *FileMQ) Read() (MQData, error) {
	if f.stop {
		return nil, fmt.Errorf("filemq is closed")
	}
	return <-f.readBuffer, nil
}

// 写入文件队列
func (f *FileMQ) Write(data MQData) error {
	if f.stop {
		return fmt.Errorf("filemq is closed")
	}
	f.writeBuffer <- data
	return nil
}

// TryRead 尝试读取文件队列 不会阻塞
func (f *FileMQ) TryRead() MQData {
	if f.stop {
		return nil
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	if len(f.readBuffer) > 0 {
		return <-f.readBuffer
	}
	return nil
}

// TryWrite 尝试写入数据 不会阻塞 如果成功则返回true. 失败则返回false
func (f *FileMQ) TryWrite(data MQData) bool {
	if f.stop {
		return false
	}

	f.lock.Lock()
	defer f.lock.Unlock()

	// 检查当前写数据是否已满
	if len(f.writeBuffer) >= f.bufferSize {
		return false
	}

	f.writeBuffer <- data
	return true
}

// ##################################################################################################

// 初始化文件队列
func (f *FileMQ) init() error {
	// 检查目标文件是否已经被队列监听
	// 获取完整文件路径
	// 获取绝对路径
	absPath, err := filepath.Abs(f.filePath)
	if err != nil {
		return fmt.Errorf("get absolute path error: %v", err)
	}

	_, ok := initMQMap.Load(absPath)
	if ok {
		// 不允许多个实例监听同一个文件
		return errors.New("mq file is already in use")
	}

	// 尝试打开文件,如果文件不存在则创建
	file, err := os.OpenFile(f.filePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := file.Sync(); err != nil {
		return err
	}

	initMQMap.Store(absPath, struct{}{}) // 记录初始化
	return nil
}

// 循环监听读取
func (f *FileMQ) goRead() {
	for {

		func() {
			time.Sleep(DEFAULT_TICK)
			f.lock.Lock()
			defer f.lock.Unlock()

			mqDatasLineMap, err := f.readMQDatas()
			if err != nil {
				log.Println("readMQDatas error:", err)
				return
			}

			for k, v := range mqDatasLineMap {
				if v == nil {
					// 结果为空,说明是不合法的消息,直接标记删除
					slog.Warn("mqdata is invalid, delete it", slog.String("file", f.filePath), slog.String("line", strconv.Itoa(k)))
					f.cleanLineMap[k] = struct{}{}
					continue
				}
				select {
				case f.readBuffer <- v:
					// 缓存未满,写入缓存
					f.cleanLineMap[k] = struct{}{} // 标记删除
				default:
					// 缓存已满,直接退出此次扫描
				}
			}
		}()
		// 确保在一次完整的扫描结束之后，再关闭监听
		if f.stop {
			f.wg.Done()
			return
		}
	}
}

// 循环监听写入
func (f *FileMQ) goWrite() {
	tt := time.NewTicker(DEFAULT_TICK)
	defer tt.Stop()
	for {
		select {
		case data := <-f.writeBuffer:
			// 写入文件
			func() {
				f.lock.Lock()
				defer f.lock.Unlock()

				err := f.writeMQData(data)
				if err != nil {
					slog.Error("write file error", "err", err)
				}
			}()
		case <-tt.C:
			if f.stop {
				f.wg.Done()
				return
			}
		}
	}
}

// 循环清理文件队列
func (f *FileMQ) goClean() {
	for {
		if f.stop {
			func() {
				f.lock.Lock()
				defer f.lock.Unlock()

				// 在关闭时清理
				err := f.clean()
				if err != nil {
					slog.Error("mq close clean file error", "err", err)
				}
			}()
			f.wg.Done()
			return
		}

		func() {
			time.Sleep(DEFAULT_TICK * 10) // 10次周期清理一次
			f.lock.Lock()
			defer f.lock.Unlock()

			if len(f.cleanLineMap) > DEFAULT_CLEAN_LINE_NUM {
				err := f.clean()
				if err != nil {
					slog.Warn("clean file error", "err", err)
				}
			}
		}()
	}
}

// ##################################################################################################

// 将数据写入文件
func (f *FileMQ) writeMQData(data MQData) error {
	// 追加模式打开文件
	file, err := os.OpenFile(f.filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// 尝试生成数据
	dataRaw, err := makeFileLine(data)
	if err != nil {
		return err
	}

	_, err = file.Write(dataRaw)
	if err != nil {
		return err
	}
	return file.Sync()
}

// 清理文件数据
func (f *FileMQ) clean() error {
	if len(f.cleanLineMap) == 0 {
		return nil
	}

	tempFilePath := f.filePath + ".temp"

	err := func() error {
		file, err := os.OpenFile(f.filePath, os.O_RDONLY, 0666)
		if err != nil {
			return fmt.Errorf("open file error: %v", err)
		}
		defer file.Close()

		// 创建临时文件
		// 创建或者重写临时文件
		tempFile, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
		if err != nil {
			return fmt.Errorf("create temp file error: %v", err)
		}
		defer tempFile.Close()

		// 逐行遍历
		line := 0
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line++
			if _, ok := f.cleanLineMap[line]; ok {
				// 标记需要删除的跳过不保存
				continue
			}
			// 将不需要清理的数据写入临时文件
			if _, err := tempFile.Write(scanner.Bytes()); err != nil {
				return fmt.Errorf("write temp file error: %v", err)
			}
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("read file error: %v", err)
		}

		if err := tempFile.Sync(); err != nil {
			return fmt.Errorf("sync temp file error: %v", err)
		}

		return nil
	}()
	if err != nil {
		return err
	}

	if err := os.Rename(tempFilePath, f.filePath); err != nil {
		return fmt.Errorf("rename temp file error: %v", err)
	}

	f.cleanLineMap = make(map[int]struct{}) // 完成清理后清空
	return nil
}

// 读取文件数据
func (f *FileMQ) readMQDatas() (map[int]MQData, error) {
	file, err := os.OpenFile(f.filePath, os.O_RDONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("open file error: %v", err)
	}
	defer file.Close()

	ans := make(map[int]MQData)

	// 逐行读取文件
	line := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line++
		if _, ok := f.cleanLineMap[line]; ok {
			// 跳过之前被标记的行
			continue
		}
		bytes := scanner.Bytes()
		mqDataType, mqData := cutFileLine(bytes)
		unmarshalFunc, ok := UnMarshalFuncMap[mqDataType]
		if !ok {
			// 目标数据的类型不受支持,不标记清除数据,直接跳过
			continue
		}
		data, err := unmarshalFunc(mqData)
		if err != nil {
			// 目标数据反序列化失败,认为此条数据格式有错误,标记删除次数据
			ans[line] = nil
			continue
		}
		if data.Check() {
			// 目标数据格式正确,添加到返回结果中
			ans[line] = data
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read file error: %v", err)
	}

	return ans, nil
}
