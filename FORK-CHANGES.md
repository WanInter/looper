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

---

## 建议
- **1（opencode `--dir`）** 与 **4（reviewer 转换）** 更像**通用缺陷**，值得提 **upstream PR** 给 `nexu-io/looper`（多账号/他人也可受益）。
- **2、3、5** 偏单账号 / 自建工作流（5 为工作流增强），可按需 upstream 或不提。
- 提交链（main）：`f7e7526` → `4bea3ee` → `103983d` → `a88545b` → `fab8076`。
