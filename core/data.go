package core

import (
	"context"
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

/*
	create database leaf-segment;
	
	CREATE TABLE `segments` (
	 `biz_tag` varchar(32) NOT NULL,
	 `max_id` bigint NOT NULL,
	 `step` bigint NOT NULL,
	 `description` varchar(1024) DEFAULT '' NOT NULL,
	 `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	 PRIMARY KEY (`biz_tag`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8;
	
	INSERT INTO segments(`biz_tag`, `max_id`, `step`, `description`) VALUES('test', 0, 100000, "test业务");
*/

type Data struct {
	db *sql.DB // 数据库连接对象
}

var DefaultData *Data //全局数据库实例

// InitData 初始化MySQL数据库连接
func InitData() (err error) {
	// 使用全局配置的 DSN (数据源名称) 初始化数据库连接
	db, err := sql.Open("mysql", DefaultConfig.DSN)
	if err != nil {
		return err
	}

	// 设置连接池的最大空闲连接数
	db.SetMaxIdleConns(10)

	// 设置连接的最大生命周期（0表示不限制）
	db.SetConnMaxLifetime(0)

	// 赋值全局数据库实例
	DefaultData = &Data{db: db}
	return nil
}

// NextId 获取并更新下一个可用的 ID 段
func (data *Data) NextId(bizTag string) (maxId int64, step int64, err error) {
	var (
		tx           *sql.Tx    // 事务对象
		query        string     // SQL 查询语句
		stmt         *sql.Stmt  // SQL 预处理语句
		result       sql.Result // SQL 执行结果
		rowsAffected int64      // 受影响的行数
	)

	// 设置 2 秒超时，防止长时间等待
	ctx, cancelFunc := context.WithTimeout(context.Background(), 2*time.Second)

	// 函数退出时取消超时上下文
	defer cancelFunc()

	// 开启事务，设置上下文以支持超时和取消
	if tx, err = data.db.BeginTx(ctx, nil); err != nil {
		return
	}

	// STEP 1: 更新 max_id，将其前进一个步长，获取一个新的 ID 段
	query = "UPDATE " + DefaultConfig.Table + " SET max_id = max_id + step WHERE biz_tag = ? "

	// 预处理查询语句
	if stmt, err = tx.PrepareContext(ctx, query); err != nil {
		goto ROLLBACK // 失败则回滚事务
	}

	// 确保 stmt 关闭，以免资源泄漏
	defer stmt.Close()

	// 执行更新操作，使用指定的业务标签
	if result, err = stmt.ExecContext(ctx, bizTag); err != nil {
		goto ROLLBACK // 执行失败则回滚
	}

	// 检查更新操作影响的行数，确保存在该业务标签的记录
	if rowsAffected, err = result.RowsAffected(); err != nil { // 获取受影响行数出错
		goto ROLLBACK
	} else if rowsAffected == 0 { // 没有找到相应的记录
		err = errors.New("biz_tag not found")
		goto ROLLBACK
	}

	// STEP 2: 查询最新的 max_id 和 step，在事务中以保证数据一致性
	query = "SELECT max_id , step " +
		" FROM " + DefaultConfig.Table + " WHERE biz_tag = ? "

	// 重新准备查询语句
	if stmt, err = tx.PrepareContext(ctx, query); err != nil {
		goto ROLLBACK
	}

	// 查询新的 max_id 和 step 值
	if err = stmt.QueryRowContext(ctx, bizTag).Scan(&maxId, &step); err != nil {
		goto ROLLBACK
	}

	// STEP 3: 提交事务，保存更新的 max_id
	err = tx.Commit()
	return

ROLLBACK:
	// 如果有任何错误则回滚事务
	tx.Rollback()
	return
}
