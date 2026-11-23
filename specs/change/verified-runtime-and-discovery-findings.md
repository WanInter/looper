# Looper 项目审查：自动合并、任务恢复与发现

审查从 [979f038](https://github.com/WanInter/looper/tree/979f038da6007264cfcd44bdbb4db3ff7750c055) 开始；提交前与更新后的 [main@e61e908](https://github.com/WanInter/looper/tree/e61e90836bace6e41fc0c0185096907273954ee2) 核对。以下各项可分别交付；“实测”指在本地运行，“代码路径”指由实现确认触发条件。每项的验收条件面向后续修复。

## 首次直接合并不检查已知的 CI 失败（实测）

在临时 Go test overlay 中，让两个现有的首次直接合并用例分别返回 `ChecksSummary="FAILURE"`：有验收条件和无验收条件的用例都仍然记录了一次固定 HEAD 的合并请求。`internal/reviewer/runner.go` 的 `maybeAutoMergeCleanReview` 和 `publishCriteriaApprovedReview` 会刷新 PR，但都没有使用刷新结果中的检查状态；同 HEAD 的后续复查则会对失败或进行中的检查延迟合并。GitHub 仍会执行分支保护；只有失败的检查非必需或仓库没有强制检查时，该请求才可能成功。这里实测的是 fake gateway 收到请求，没有合并真实 PR。

**验收**：在这两条首次发布路径及同 HEAD 复查中，使用同一套检查状态规则。明确失败或进行中时不发送直接合并请求，转绿后只向已评审的 HEAD 合并一次；另行覆盖没有配置 CI 的仓库。

## 后补标签后，Reviewer 自己的说明评论会阻止合并（实测）

实现 PR 的关联 issue 已被跟踪，但首次干净评审时 PR 缺少 `looper:*` 标签。Reviewer 会在会话中说明拒绝原因 `scope`。在 `TestSameHeadAutoMergeAfterLabelChange/waiting_discovery` 中，仅让 `ListIssueComments` 读回 `CreateIssueComment` 实际写出的这条说明，再补标签，合并请求数就从预期的 1 变为 0。`internal/reviewer/same_head_automerge.go` 的反馈过滤器把这条非空、新于评审开始时间的说明当成需要人工处理的反馈。现有 fake 的写入和读取分用两个列表，原用例因此通过。

**验收**：让测试网关读回自己写入的评论；同一 Reviewer 发布的那条可核验的拒绝说明可以排除，但新的人工留言、其他作者的同文留言、被修改的反馈仍须阻止复用旧评审。不得仅按作者或任意 `looper:` 字样忽略评论。

### 同一决策的路由、配置与可观测性（代码路径）

- **Forgejo 首次直接合并的路由与复查不同。** `internal/runtime/scheduler.go` 的 `EnableAutoMerge` 会根据项目调用 Forgejo；`MergePullRequest` 则无条件转给 GitHub gateway。Forgejo 原生评审模式且首次发布走到 `directMerge` 时，两条发布路径使用后者；同 HEAD 复查会为 Forgejo 选前者。验收：使用本地 Forgejo fixture 和记录调用的假 `gh`，确保首次发布及复查均只调用配置的 provider，或在不支持该配置时明确拒绝。
- **配置的 `COMMENT` 与自审降级的 `COMMENT` 不是同一条路径。** `maybePublishCriteriaAnchoredCleanReview` 要求有效 clean 策略为 `APPROVE`，其唯一调用的 `publishCleanReviewWithoutCriteria` 却又设计了一个非 `APPROVE` 的合并分支。配置固定时，外层不会把这类调用传给内层。验收：明确首次发布时显式 `clean=COMMENT` 的产品约定，覆盖显式 `COMMENT`、配置 `APPROVE` 但自审时降级、以及同 HEAD 补标签三种场景，不以单条 `COMMENT` 作为合并授权。
- **运维摘要没有区分合并成功和仍有阻塞。** `reconcileSameHeadAutoMerge` 针对新人工留言或未解决线程会返回 `nil`，成功启用/完成合并也返回 `nil`；`internal/reviewer/runner.go` 的调用方对这些结果都写入相同的 `SkipReason`。验收：让操作者能从单次运行摘要区分“已启用 / 已合并”和“未合并及原因”，而不改变或消耗原有重试证据。

## 普通人工答复可能已经保存，任务却不再运行（实测）

[`deliverHITLAnswerToLoopWithCaps`](https://github.com/WanInter/looper/blob/979f038/internal/runtime/hitl_github_poll.go#L923-L967) 在处理普通问题时先将答复写成 `answered`、loop 改成 `running`，再单独重新入队。两次写入之间失败，下一次投递会因为 loop 已不是 `awaiting_human` 而直接返回。这与同函数中 budget/scope 答复的事务路径不同。

验证方法：在临时 SQLite 库中准备一个等待答复的 loop 和一条已取消的队列项，用一次性测试触发器拒绝将该项改为 `queued`。第一次投递返回数据库错误；解除故障后再次投递返回成功，读回状态仍是 `running / answered / cancelled`。`internal/storage/repositories.go` 的 `RequeueLatestCancelledByLoop` 还允许返回 `(0, nil)`；这既可能表示已有活跃项，也可能表示根本没有项可恢复。

**验收**：答复与恢复调度在同一事务内完成；入队失败后仍能重投；成功重试恰好留下一个可执行项。分别覆盖缺失队列项和已有活跃队列项，避免将合法去重当成错误。

## CLI 重启可能比 daemon 的正常排空提前超时（代码路径）

在 foreground/PID 文件模式下，`internal/cliapp/daemon_runtime.go` 的 `stopDaemonProcess` 发出 SIGTERM 后只等 2 秒，而 `daemonRestart` 遇到超时就返回，不会启动替代进程。旧 daemon 仍可能在第 3 秒正常退出，留下没有 daemon 运行的空档。

**验收**：用假进程模拟第 3 秒正常退出，CLI 应在配置的停机预算内成功重启，并且只在确认旧进程退出后启动新实例；真正耗尽预算时，应给出能区分“仍在排空”与“已停止”的诊断。

## 区分停机取消与持久化故障（代码路径）

`internal/agent/executor.go` 的 `runCheckpointFallback` 在重新绑定进程后，用可被取消的执行 context 写新的 `running` 记录。若此时取消，写入失败会触发回收，并经 `reapOnOwnershipPersistFailure` 上报持久化错误。这个窗口与重新绑定前就已取消不同；尚未实测它在重启后会产生哪种恢复状态。

**验收**：在重新绑定成功与写入之间插入取消，核对旧记录、实际终态和告警。只有确认进程组退出且终态一致时才将预期取消与硬故障区分；真正的数据库故障仍上报。

### 新排空实现的两个边界（`e61e908` 代码路径）

- `ConfiguredExecutor.Start` 在初次绑定句柄后写 `running` 失败，会把启动错误传给 `WaitFinalized`。若调用方 context 此时被取消、确实没有活动记录且进程组已确认退出，停机仍可能被记为排空失败，导致 `Runtime.Stop` 保留存储。验收：用同步点精确覆盖绑定成功后写入前的取消；只有能确认进程已退出且没有非终态记录时，才区分调用方取消与真正的终态持久化失败。
- `launchdExitTimeout` 为 launchd 设置固定加配置项的总窗口，但 `ActiveExecutionRegistry.drainNonAgentHandles` 在公共等待期用尽后，会串行给**每个**残留非 agent 句柄一个新的 `killBudget()`。因此这段代码的最坏等待时间随残留句柄数增长；常规迅速退出不触发该路径。验收：多个被跟踪的非 agent 命令同时无法及时退出时，强制排空遵守同一个总截止时间，并保留每个未确认退出的诊断。此处未观测到实际的 launchd 强杀。

## 同标签任务可能被首页中其他人的任务遮住（代码路径）

普通 GitHub 轮询允许 Planner / Worker 接受未指派任务。`internal/planner/runner.go` 和 `internal/worker/runner.go` 的 `DiscoverIssues` 因此先按标签取候选，再在本地检查 assignee。`internal/infra/github/gateway.go` 的 `listOpenIssuesRaw` 默认最多取 30 项；发现快照也先截取候选再返回。若相同标签的前 30 项均指派给他人，第 31 项即使指派给自己或未指派，后续轮询在排序稳定时仍看不见它。

**验收**：用固定排序的 fake provider 构造上述两种第 31 项；自动轮询能在有界轮次内发现它，同时不为前 30 项入队。保留标签、hold 与去重语义，验证每轮请求量有界。

## 两项 Forgejo 契约测试仍断言旧指派规则（实测）

`FORK-CHANGES.md` §2 记录了“未指派也接单、明确指派给他人则跳过”的行为。`internal/runtime/hosting_identity_contract_test.go` 仍要求每个请求的 `assignee` 参数等于当前角色；实际请求的身份分别是 `planner-bot` 和 `worker-bot`，参数均为空。`internal/runtime/webhook_forgejo_contract_test.go` 的 fixture 中，未指派的 issue 11 带有 `looper:worker-ready`；实测因此有两个 worker loop，断言只允许一个。

可单独复现：

```sh
go test ./internal/runtime -count=1 \
  -run '^(TestHostingIdentityRoleDiscoveryUsesSelectedForgejoAccount|TestForgejoWebhookIngressCreatesRoleQueuesWithoutPollingOrGH)$'
```

**验收**：按实际目标 issue 和角色身份断言，不只改预期总数；覆盖“自己 / 未指派 / 明确他人”、hold、关闭、重复投递和不同角色凭据，且保留原有的错误、权限检查。

## 开启 GitHub 人工答复轮询后，每个项目都读取所有历史 loop（代码路径）

`internal/runtime/scheduler.go` 的 `runDefaultSchedulerTick` 对每个参与发现的项目调用 `runGitHubHITLPoll`。后者在 `internal/runtime/hitl_github_poll.go` 用 `Repos.Loops.List` 取全部记录，再筛项目和 `awaiting_human`；送出新问题后还可能再取一次。即使没有待答复项，P 个项目、L 条历史记录也会在一轮中读取约 P×L 条记录。这是读放大，尚未测量生产延迟。

**验收**：查询只返回该项目需要处理的候选，或一次获取候选后按项目分组；覆盖待发送、已发送、同 PR 多 loop 的场景。用多项目及大量终态历史的基准，比较查询返回的行数和分配量。

## CI 与本地测试环境（工具链）

- **重复执行 daemon boot 测试。** `.github/workflows/ci.yml` 的 `verify` 运行 `go test ./...`，常驻 smoke 再运行 daemon boot 测试，命中路径时 `daemon-boot-e2e` 第三次运行同一组。`internal/e2e/main_test.go` 会进入构建测试二进制的 `TestMain`。验收：为这组用例指定明确的 CI 执行入口，保留每个 PR 的启动验证、必要的 required check 和失败产物；用 Actions 运行记录确认不再重复。竞态检测目前只单列了 `hostingidentity`，其覆盖范围本身不构成已发现的数据竞争。
- **Dashboard 主题测试依赖 Node 的 Web Storage 环境。** 本机 Node `v26.11.1` 上运行 Vitest：194 项中 188 通过、6 项在 `web/dashboard/src/lib/theme.test.ts` 的 `window.localStorage.clear()` 失败；同版本加 `NODE_OPTIONS=--no-experimental-webstorage` 单跑主题测试，10/10 通过。CI 使用 Node 22。验收：显式提供独立的测试存储或固定并说明支持的测试环境；保留对主题持久化行为的断言。此结果不能推出浏览器主题功能失效。
