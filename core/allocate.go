package core

import (
	"errors"
	"sync"
	"time"
)

// Segment 号段结构体定义了号码池的号段范围
type Segment struct {
	offset int64 // 当前消费偏移量，指示已经分配到的号段位置
	left   int64 // 号段左边界（包含）
	right  int64 // 号段右边界（不包含）
}

// BizAlloc 管理与特定业务标识（bizTag）相关的号段分配
type BizAlloc struct {
	mutex        sync.Mutex  // 互斥锁，保证并发安全
	bizTag       string      // 业务标识，用于区分不同的号段池
	segments     []*Segment  // 双Buffer, 最少0个, 最多2个号段在内存
	isAllocating bool        // 是否正在分配中(远程获取)
	waiting      []chan byte // 因号码池空而挂起等待的客户端
}

// Alloc 全局分配器, 管理所有的biz号码分配
type Alloc struct {
	mutex  sync.Mutex           // 互斥锁，保证并发安全
	bizMap map[string]*BizAlloc // 存储各业务号段池的映射
}

// DefaultAlloc 是全局分配器实例
var DefaultAlloc *Alloc

// InitAlloc 初始化全局分配器
func InitAlloc() (err error) {
	DefaultAlloc = &Alloc{
		bizMap: map[string]*BizAlloc{}, // 初始化业务号段映射
	}
	return
}

// leftCount 计算BizAlloc中剩余的未分配号码数量
func (bizAlloc *BizAlloc) leftCount() (count int64) {
	for i := 0; i < len(bizAlloc.segments); i++ {
		count += bizAlloc.segments[i].right - bizAlloc.segments[i].left - bizAlloc.segments[i].offset
	}
	return count
}

// leftCountWithMutex 在锁保护下计算剩余未分配号码数量
func (bizAlloc *BizAlloc) leftCountWithMutex() (count int64) {
	bizAlloc.mutex.Lock()
	defer bizAlloc.mutex.Unlock()
	return bizAlloc.leftCount()
}

// newSegment 请求数据库获取一个新的号段
func (bizAlloc *BizAlloc) newSegment() (seg *Segment, err error) {
	var (
		maxId int64 // 数据库返回的最大ID
		step  int64 // 每次获取的号段大小
	)

	// 通过数据库获取号段范围
	if maxId, step, err = DefaultData.NextId(bizAlloc.bizTag); err != nil {
		return
	}

	seg = &Segment{}
	seg.left = maxId - step // 新号段左边界
	seg.right = maxId       // 新号段右边界

	return
}

// wakeup 唤醒所有等待分配号段的客户端
func (bizAlloc *BizAlloc) wakeup() {
	var (
		waitChan chan byte
	)
	for _, waitChan = range bizAlloc.waiting {
		close(waitChan) // 关闭通道来唤醒等待者
	}
	bizAlloc.waiting = bizAlloc.waiting[:0] // 清空等待队列
}

// 分配号码段, 直到足够2个segment, 否则始终不会退出
func (bizAlloc *BizAlloc) fillSegments() {
	var (
		failTimes int64    // 连续分配失败次数
		seg       *Segment // 新的号段
		err       error
	)
	for {
		bizAlloc.mutex.Lock()
		if len(bizAlloc.segments) <= 1 { // 只剩余<=1段, 那么继续获取新号段
			bizAlloc.mutex.Unlock()

			// 请求数据库获取新的号段
			if seg, err = bizAlloc.newSegment(); err != nil {
				failTimes++
				if failTimes > 3 { // 连续失败超过3次则停止分配
					bizAlloc.mutex.Lock()
					bizAlloc.wakeup() // 唤醒等待者, 让它们立马失败
					goto LEAVE
				}
			} else {
				failTimes = 0 // 分配成功则失败次数重置为0
				// 新号段补充进去
				bizAlloc.mutex.Lock()
				bizAlloc.segments = append(bizAlloc.segments, seg) // 添加新号段
				bizAlloc.wakeup()                                  // 尝试唤醒等待资源的调用
				if len(bizAlloc.segments) > 1 {                    // 已生成2个号段, 停止继续分配
					goto LEAVE
				} else {
					bizAlloc.mutex.Unlock()
				}
			}
		} else {
			break // never reach
		}
	}

LEAVE:
	bizAlloc.isAllocating = false
	bizAlloc.mutex.Unlock()
}

// popNextId 弹出下一个未分配的ID
func (bizAlloc *BizAlloc) popNextId() (nextId int64) {
	nextId = bizAlloc.segments[0].left + bizAlloc.segments[0].offset
	bizAlloc.segments[0].offset++
	if nextId+1 >= bizAlloc.segments[0].right {
		bizAlloc.segments = append(bizAlloc.segments[:0], bizAlloc.segments[1:]...) // 弹出第一个seg, 后续seg向前移动
	}
	return
}

// nextId 获取下一个分配的ID
func (bizAlloc *BizAlloc) nextId() (nextId int64, err error) {
	var (
		waitChan  chan byte
		waitTimer *time.Timer
		hasId     = false
	)

	bizAlloc.mutex.Lock()
	defer bizAlloc.mutex.Unlock()

	// 1, 有剩余号码, 立即分配返回
	if bizAlloc.leftCount() != 0 {
		nextId = bizAlloc.popNextId()
		hasId = true
	}

	// 2, 段<=1个, 启动补偿线程
	if len(bizAlloc.segments) <= 1 && !bizAlloc.isAllocating {
		bizAlloc.isAllocating = true
		go bizAlloc.fillSegments()
	}

	// 分配到号码, 立即退出
	if hasId {
		return
	}

	// 3, 没有剩余号码, 此时补偿线程一定正在运行, 等待其至多一段时间
	waitChan = make(chan byte, 1)
	bizAlloc.waiting = append(bizAlloc.waiting, waitChan) // 排队等待唤醒

	// 释放锁, 等待补偿线程唤醒
	bizAlloc.mutex.Unlock()

	waitTimer = time.NewTimer(2 * time.Second) // 最多等待2秒
	select {
	case <-waitChan: // 等待唤醒
	case <-waitTimer.C: // 超时
	}

	// 4, 再次上锁尝试获取号码
	bizAlloc.mutex.Lock()
	if bizAlloc.leftCount() != 0 {
		nextId = bizAlloc.popNextId()
	} else {
		err = errors.New("no available id")
	}
	return
}

// NextId 获取指定业务的下一个ID
func (alloc *Alloc) NextId(bizTag string) (nextId int64, err error) {
	var (
		bizAlloc *BizAlloc
		exist    bool
	)

	alloc.mutex.Lock()
	if bizAlloc, exist = alloc.bizMap[bizTag]; !exist { // 如果bizTag不存在
		bizAlloc = &BizAlloc{
			bizTag:       bizTag,
			segments:     make([]*Segment, 0),
			isAllocating: false,
			waiting:      make([]chan byte, 0),
		}
		alloc.bizMap[bizTag] = bizAlloc // 新建并存入映射
	}
	alloc.mutex.Unlock()

	// 从业务号段池获取下一个ID
	nextId, err = bizAlloc.nextId()

	/*
		Leaf-segment方案可以生成趋势递增的ID，同时ID号是可计算的，不适用于订单ID生成场景，
		比如竞对在两天中午12点分别下单，通过订单id号相减就能大致计算出公司一天的订单量，这个是不能忍受的。

		其实ID可以是：符号位+机器ID+业务ID+毫秒时间戳+nextId

	*/
	nextId = nextId + time.Now().UnixMilli()
	return
}

// LeftCount 获取业务池中的剩余号码数量
func (alloc *Alloc) LeftCount(bizTag string) (leftCount int64) {
	var (
		bizAlloc *BizAlloc
	)

	alloc.mutex.Lock()
	bizAlloc, _ = alloc.bizMap[bizTag]
	alloc.mutex.Unlock()

	if bizAlloc != nil {
		leftCount = bizAlloc.leftCountWithMutex()
	}
	return
}
