# looper fork 变更说明（WanInter/looper）

基座上游：`nexu-io/looper`。本 fork（`WanInter/looper`）针对**单账号 + 自建部署**场景修复了 4 处，均为「改 = 能跑起来」的必需修复。

---

## 1. `f7e7526` — fix(agent): opencode v2+ 无 `--dir`
- **问题**：looper 给 opencode 拼 `opencode run --dir <worktree> <prompt>`，但 opencode v2+ 已移除 `--dir`（目录由位置参数 / 进程 cwd 决定）→ 每次 run 报 `Unrecognized flag: --dir`，**全部失败**。
- **修法**：`internal/agent/executor.go` 的 `resolveOpenCodeArgs` 与 `resolveNativeResumeArgs` 的 opencode 分支去掉 `appendDirFlag`（looper 已把进程 `cmd.Dir` 设为 worktree，opencode 直接用 cwd）。
- **影响**：本机默认引擎 opencode 可正常执行。补丁另存 `~/.looper/patches/opencode-no-dir.patch`。

## 2. `4bea3ee` — feat(triggers): 未指派 ＝ 视作指派给当前用户
- **问题**：`roles.*.triggers.requireAssigneeCurrentUser=true` 时，发现查询带 `--assignee <me>` 且门禁要求含我 → **无 assignee 的 issue 永远不接单**。
- **修法**：发现查询不再带 `--assignee <me>`（未指派的触发标签 issue 也能取到）；由 `shouldClaimIssue` / `shouldClaimWorkerIssue` 门禁决定——**未指派 → 接单**，**显式指派给他人 → 跳过**。
- **文件**：`internal/planner/runner.go`、`internal/worker/runner.go`（各带单测 `zz_unassigned_assignee_test.go`）。

## 3. `103983d` — feat(notify): 富飞书卡片 + info/success 级
- **问题**：looper 向飞书汇报**过于简略**（info 级一律被 `level filtered` 吞掉；无任务锚点）。
- **修法**：
  1. `notifications.webhook.levels` 接受 `info` / `success`（config 类型 + 校验）。
  2. **每个任务都建/刷新飞书锚点富卡片**（📋 Issue/PR 链接 · 仓库 · 标题 · 里程碑时间线 · 耗时 · `looper resume <seq>` 接管提示），并将「锚点卡片 + 里程碑」与 level 过滤**解耦**——即使文本 ping 被过滤，卡片照建照刷。
  3. `RecordMilestone` 跳过连续重复条目。
- **文件**：`internal/config/types.go`、`internal/config/validate.go`、`internal/infra/notify/gateway.go`。

## 4. `a88545b` — fix(reviewer): clean COMMENT 也能转 `spec-ready`
- **问题**：单账号下 looper **无法 APPROVE 自己的 PR**（GitHub 禁止自审），因此把 clean 事件配成 `COMMENT`；但 `spec-ready` 标签转换原本**只在 `clean==APPROVE` 时触发** → **spec PR 永远停在 `looper:spec-reviewing`**（转换被禁用）。
- **修法**：放宽 `cleanSpecLabelTransitionAllowed` 为「**clean 结果 + 配置的 clean 事件**」即满足转换（不再硬性要求 APPROVE）；同步更新 2 个断言旧行为的单测。
- **验证**：companion-robot **#61/#62**、trans-api-admin **#89/#90/#91/#100–103** 已自动转 `looper:spec-ready`。
- **配套（配置，非代码）**：`[roles.reviewer.behavior.reviewEvents]` 的 `blocking` 与 `clean` **都设 `COMMENT`**（否则评审贴不上、报 `no matching GitHub review marker` 失败）。

## 5. `fab8076` — feat(reviewer): spec-ready 自动交接实现（闭环）
- **问题**：spec PR 转 `looper:spec-ready` 后**没有自动桥**接 Worker（Worker 只认 issue 的 `looper:worker-ready`，其源自 issue 的 `dispatch/implement`）→ 流程断在 spec-ready，需人工把 issue 改成 `dispatch/implement` 才继续。
- **修法**（`autoImplement=true` 时）：clean spec 评审把 PR 转 `spec-ready` 的同时，解析关联 issue（PR body 的 `Refs/Closes #N`，或分支名 `looper/planner/<N>-…`）→ 给 issue **加 `dispatch/implement`**、**删 `dispatch/plan` 与 `looper:plan`** → 自主 Coordinator 自动派 Worker 实现，**真正闭环**。
- **配置**：`[roles.reviewer.behavior.reviewEvents] autoImplement = true`（默认 false；已在本机开启）。
- **文件**：`internal/config/{types,normalize,hot_reload}.go`、`internal/reviewer/runner.go`（+ 单测）、`internal/runtime/scheduler.go`（适配器补 `AddIssueLabels`）。
- **补充 `76c00b0`**：交接时**同时加 `looper:worker-ready`**（Worker 的真实发现触发标签）——因为 Coordinator 的 `decideAutonomous` 还要求 issue 有 `triaged`，spec 类 issue 常缺该标签，直接加 `looper:worker-ready` 可让 Worker 立刻实现、不依赖 Coordinator 闸门。
- **活体验证**：spec PR #62 clean → 转 `spec-ready`，关联 issue **#21** 标签由 `['looper:plan']` → `['dispatch/implement','looper:worker-ready']`；`looper work --issue 21` → **Worker 启动**（"Implement specs/2026-11-22-21-p2-wal-checkpoint.md"，loop #65 `execute` running）✅

## 6. `9f2c9be` — fix(coordinator): 永不向 PR 作者请求评审
- **问题**：`enableSelfReview=true`（单账号）后，`reviewAssignmentEligible` 不再排除作者 → Coordinator 发现阶段对「自己」的 PR 请求评审 → GitHub **422 `Review cannot be requested from pull request author`** → **整个 scheduler tick 失败**（发现 + 派单中断，日志 `looperd scheduler tick failed`）。
- **修法**：`reviewAssignmentEligible` **永远排除作者**（请求自审本就不可能）；自审改由 reviewer 的**标签发现**处理。
- **另注（运维，非代码）**：`WanInter/looper` 曾被注册为监控项目，但该仓 **Issues 关闭** → `planner/worker discovery failed … has disabled issues` → 同样拖垮每个 tick。已 `looper project remove looper --force`。

---

## 7. fix(reviewer): 同一 HEAD 补加标签后重新判断自动合并
- 已完成干净评审的实现 PR，补上 `looper:*` 标签后，可通过重试或等待中的发现流程重新判断合并；不重跑模型、不重发评审、不增加发布计数。
- 合并依据是精确匹配 HEAD 和发布 ID 的成功检查点，以及已验证的评审标记或发布时记录的干净 no-op。`COMMENT` 本身不是证据；没有正向证据的旧 no-op 不会自动合并。只在合并或启用自动合并成功后记录发布去重 ID；临时拒绝不会消耗发布证据。
- 合并前复查 PR、关联 issue、仓库策略和人工暂停；任何未解决的评审线程（不限于 Looper）均阻止合并。新出现或编辑过的非空 `COMMENT` 评审及 PR 会话留言也会阻止复用旧证据，不猜测自由文本是否表示同意。继续使用 CI 检查和固定 HEAD 的合并调用。
- 重新发现只安排合并检查，沿用 reviewer quiet period（设为 0 时，此路径仍等待一分钟），不挤占已入队的评审工作。回归测试 `TestSameHeadAutoMergeAfterLabelChange` 验证线程解决、留言撤除、issue hold 解除和仓库策略恢复后可重试成功，且不重跑模型、不重复发布；原有安全场景继续覆盖。

## 8. fix(runtime): 有序停机后恢复中断的运行
- 关闭接单后，Supervisor 取消 agent 和队列租约，并等待当前 daemon 拥有的进程组排空。角色的 `Wait(ctx)` 可能先因取消返回；停机另外等待执行器确认 containment 并把终态提交到 SQLite。失败或超时仍按原有的 drain failure 路径报告、保留存储，不用 PID 消失代替证据。
- 启动仍遵循 ADR-0015 R8 / #581：没有持久终态的外部孤儿保持隔离，不接管或发送信号。正常停机的中断运行沿用原有检查点和队列恢复逻辑；已排队的中断运行再次启动不会重复重排或重置计划时间。
- 新生成的 LaunchAgent plist 为 drain/落盘预留 `ExitTimeOut`，`bootout` 的 CLI 等待预算也相应延长。这不改变已安装 plist，不迁移旧的隔离执行记录。OpenCode v2 用 `--standalone` 启动私有服务（包括原生会话续跑和 fallback）；不支持 `--server`/`--attach`，不停止用户的共享服务。进程组不覆盖自行脱离该组的程序或外部服务。
- Reviewer 续跑仍检查 PR HEAD 和工作树，仅清理原有保留命名空间的临时评审产物。有普通改动则保留并报告阻塞路径；已准备的同一 checkout 若新添了本地 commit，也不会为续跑而重置。
- 验证：`TestRuntimeRestartJoinsAgentFinalizationBeforeRequeue` 用临时 SQLite 和带子进程的假 OpenCode 验证停机等待、检查点、重复恢复及单次认领；`TestStartupRecoveryExitedLeaderDoesNotConfirmDeadOrAct` 覆盖未知 containment；`TestRunPrepareWorktreeStepRealGateway*` 用本地 Git 验证临时产物和本地编辑的处理。

## 9. 规划合并、Worker 交付与 issue 收尾

- GitHub 项目启用 `roles.reviewer.behavior.reviewEvents.autoImplement` 与 Worker 自动发现后，交接依据是本地 Planner 与 issue、PR 的持久关联，以及远端的合并结果、目标分支和实际规格文件。已合并的规划 PR 不接受后续推送；规格交付本身不会关闭 issue。人工暂停、归档项目不自动推进。
- Worker 创建实现 PR 和推送至既有 PR 使用同一交付记录。PR 正文使用 `Refs`，规格链接和人工正文保留；只有 Looper 管理的段落会被替换。`implementationDelivery` 放在既有 loop metadata / run checkpoint，无需迁移数据库。旧运行没有可核验的证据时不会被补记为完整交付。
- 完整解决须绑定唯一 issue、已交付的 HEAD、实际实现提交及该版本的验证结果。`completed` 只描述本地运行。收尾按实际合入目标分支的结果处理（包含 squash/rebase）；部分实现、仅文档、未知或有冲突的关联不自动关单。采用独立的合并事实与收尾进度，是为了在正文、标签或关闭请求失败后续办；代价是需要保留并检查旧 checkpoint 的兼容和重试路径。
- 运维核对：查看 issue 是否仍开放、是否 hold、已绑定 PR 的合并目标和交付 HEAD，再检查对应 Worker 的活跃队列及验证记录。`looper:worker-ready` 已存在只代表可被发现；不能据此认定已派单或已交付。功能不需要也不建议通过手改 SQLite 或回填旧运行的 `complete` 来启用。
- 验证：`go test ./...`、`go vet ./...` 和 `go build ./...`；`go test -race ./internal/runtime -run '^TestVerifiedIssueDeliveryLifecycle$' -count=1 -v` 在临时 SQLite、真实 Git 裸远端及模拟 GitHub 上跑通“已合并规格 → 唯一 Worker → 验证并发布 → 人工合并 → 重启后只关闭一次，清理阶段标签”。`TestPublicationResumesAfterReadbackFailure` 检查正文重试不重跑模型或重复提交。未在业务仓执行写入型沙箱测试、未接入生产 daemon。

## 建议
- **1（opencode `--dir`）** 与 **4（reviewer 转换）** 更像**通用缺陷**，值得提 **upstream PR** 给 `nexu-io/looper`（多账号/他人也可受益）。
- **2、3、5** 偏单账号 / 自建工作流（5 为工作流增强），可按需 upstream 或不提。
- 提交链（main）：`f7e7526` → `4bea3ee` → `103983d` → `a88545b` → `fab8076`。
