# hotredeploy

进程内应用热重发布库（Go）。上层直接调用；所有发布状态只保存在内存中，无持久化、无端口、无外部依赖。每个应用同一时刻最多一个**现役（active）**实例。

## 语义

- `Publish(app, version, hooks)` **立即受理**并返回该应用内从 1 开始递增的发布序号，不等待启动。上层负责校验版本，并提供进程内的三个回调：
  - `Start(ctx)`：启动实例，可能缓慢或失败；ctx 被取消表示该候选已永久失去接管资格。
  - `Serve(ctx, req)`：在已启动实例上处理一个请求，可能长期不返回。
  - `Release()`：释放实例资源。
- 一个候选在**接管之前**被更新发布受理即永久失效（状态 `superseded`）：之后即使启动成功也只释放，绝不恢复资格、绝不接管。已接管版本不受影响。
- 合格候选启动成功后接管后续请求，原现役进入 `draining`；启动期间现役继续服务。候选启动失败则保留现役，不会改用任何已失效候选。
- 首个版本就绪前，`Serve` 立即返回 `ErrUnavailable`。
- 每个请求只交给一个版本：切换前交给旧版的继续在旧版执行，切换后接入的只交给新版；不转交、不自动重试、不重复执行。
- 排空时限可通过 `WithDrainTimeout` 配置，默认 30 秒，从该版本被替换时刻起算，多个旧版本各自独立计时：
  - 全部在途请求提前完成 → 立即释放旧实例；
  - 到期仍有未完成请求 → 等待中的调用方收到 `ErrDrainTimeout`，同时释放旧实例，不等待处理函数自行返回；处理函数晚到的结果不会再交付给调用方。
- 所有“被淘汰且成功启动”的实例最终恰好释放一次（`Release` 由 `sync.Once` 保护）；请求完成、晚到启动结果、排空超时都不会改变当前现役。
- 调用方的 ctx 提前取消只表示它不再等结果，请求仍在原实例上继续执行；排空截止不受影响。
- `State(app)` 通过 `active` / `candidates`（含 `candidate` 与 `superseded`）/ `draining` 区分版本，并给出在途数与排空截止时间。
- `Close()` 停止受理新发布与请求，并按排空超时语义立即释放所有已启动实例；可重复调用。

## 安装与使用

```go
import "hotredeploy"

m := hotredeploy.NewManager(hotredeploy.WithDrainTimeout(10 * time.Second))
defer m.Close()

r, err := m.Publish("checkout", "v1", hotredeploy.AppHooks{
    Start:   inst.Start,
    Serve:   inst.Serve,
    Release: inst.Release,
})
// r.Seq 在启动完成前就已返回

resp, err := m.Serve(ctx, "checkout", req) // 就绪前返回 ErrUnavailable
st, ok := m.State("checkout")
```

完整可运行示例见 `example_test.go`。

## 测试

```bash
go test ./... -race -count=1
```

重点场景：

- `TestReverseStartupOrderNewestFails`：v2/v3 连续受理后启动结果反序到达，最新候选 v3 失败、已失效 v2 晚到启动成功；现役 v1 不变，失效候选不被启用，v2 仅释放一次。
- `TestMultipleDrainsTimeoutEachOneOutcome`：v1/v2 同时排空、截止时间独立，处理函数忽略取消并晚到结束；每个请求只得到一次 `ErrDrainTimeout` 终态，两个实例各恰好释放一次，现役 v3 不受影响，晚到结果不交付。
- 另含就绪前不可用、候选失败保留现役、排空提前完成立即释放、空实例立即释放、序号隔离、`Close` 释放等测试。
