# hotdeploy

进程内应用热重发布库。所有发布状态只保存在内存中；每个应用由一个
`Manager` 管理，任意时刻最多一个**现役（active）**版本对外服务。

上层负责校验版本、并提供“启动 / 处理请求 / 释放资源”的能力，本库负责
按规则驱动它们。不解析应用包，不依赖数据库、缓存、消息队列或端口。

## 核心语义

- `Publish(version)` 受理后**立即**返回应用内递增序号，不等启动完成。
- 每次新受理会让“尚未接管”的候选**永久失效**：它之后启动成功也只会被
  释放，绝不能接管；已经接管的版本不受影响。
- 合格候选启动成功即接管后续请求，原现役进入**排空（draining）**；
  启动期间现役继续服务；候选启动失败则保留现役。
- 初次发布就绪前，`Serve` 立即返回 `ErrUnavailable`。
- 每个请求只交给一个版本：切换前的在途请求继续在旧版跑完，切换后的新
  请求只交给新版；不会转交、重试或重复执行。
- 排空时限可配（`Config.DrainTimeout`，默认 30s），从版本被替换时起算：
  - 在途请求全部完成 → 立即释放旧实例；
  - 到期仍未完成 → 在途 `Serve` 返回 `ErrDrainTimeout` 并释放旧实例，
    不等处理自行返回；之后迟到的处理结果被丢弃，不再交付调用方。
- 排空期间可以继续发布，多个旧版本并行排空，各自截止时间独立。
- 每个成功启动的实例**恰好 `Release()` 一次**；旧请求完成、晚到的启动
  结果、排空超时都不会改变当前现役版本。

## 安装与 API

```go
import "hotdeploy"
```

实现两个接口：

```go
type Launcher[RQ, RS] interface {
    Start(ctx context.Context, version string) (Instance[RQ, RS], error)
}

type Instance[RQ, RS] interface {
    Handle(ctx context.Context, req RQ) (RS, error)
    Release()
}
```

也可用 `LauncherFunc` 直接传入函数。方法：

- `New[RQ, RS](launcher, Config) *Manager`
- `Publish(version string) int`：受理发布，返回递增序号（`Close` 后返回 -1）。
- `Serve(ctx, req) (RS, error)`：把请求分派给当前现役版本。
- `Status() Status`：区分 `Active` / `Candidate` / `Draining`，并附完整
  `History`（按受理顺序）。
- `Close()`：关闭管理，唤醒所有在途 `Serve`（返回 `ErrUnavailable`），
  并把已经启动的实例各释放一次；幂等。

## 使用示例

```go
m := hotdeploy.New[string, string](launcher, hotdeploy.Config{
    DrainTimeout: 30 * time.Second, // 0 使用默认 30s
})

seq := m.Publish("v1")   // 立即返回，不等待启动
rs, err := m.Serve(ctx, "hello")

switch {
case errors.Is(err, hotdeploy.ErrUnavailable):  // 尚无就绪版本 / 已关闭
case errors.Is(err, hotdeploy.ErrDrainTimeout): // 旧版排空超时
}
```

完整可运行示例见 `example_test.go` 中的 `ExampleManager`。

## 注意事项

- `Start` 使用内部 `context.Background()` 驱动：库不能取消一个库无法预知
  何时安全中止的启动；候选失效后若启动迟到成功，会被立即释放。
- `Release` 在独立 goroutine 中调用，避免用户代码持锁阻塞内部状态。
- `Serve` 在版本被强制排空时返回超时，但其内部 `Handle` goroutine 仍会
  运行到 `Handle` 自行返回（实例资源的释放语义由 `Release` 保证）。

## 测试

```sh
go test -race ./...
```

覆盖：候选反序启动且最新失败、多版本并行排空（完成/超时）、就绪前不可用、
无在途请求即时释放、`Close` 恰好释放一次等。
