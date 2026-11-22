package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/nexu-io/looper/internal/agent"
	"github.com/nexu-io/looper/internal/config"
	"github.com/nexu-io/looper/internal/storage"
)

func TestRuntimeRestartJoinsAgentFinalizationBeforeRequeue(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	t.Setenv("HOME", dir)
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	shim := `#!/bin/sh
[ "$1" = --standalone ] || exit 12
echo spawned >> spawns
if [ -e first-ready ]; then exit 0; fi
/bin/sleep 30 &
touch first-ready
wait "$!"
`
	if err := os.WriteFile(filepath.Join(dir, "opencode"), []byte(shim), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.DefaultConfig(dir)
	mustRestartOK(t, err)
	cfg.Storage.DBPath = filepath.Join(dir, "restart.sqlite")
	backup := filepath.Join(dir, "backups")
	cfg.Storage.BackupDir = &backup
	now := time.Now().UTC()
	at := formatJavaScriptISOString(now)
	const loopID, runID, executionID, queueID = "loop", "run", "execution", "queue"
	const checkpoint = `{"resumePolicy":"advance_from_checkpoint","snapshot":{"headSha":"review-head"}}`
	start := func() *Runtime {
		rt := New(Options{Config: cfg, Now: func() time.Time { return now }, RunSchedulerTick: func(context.Context, Services) error { return nil }})
		mustRestartOK(t, rt.Start(ctx))
		t.Cleanup(func() { rt.Stop("cleanup") })
		return rt
	}
	first := start()
	repos := first.Services().Repositories
	mustRestartOK(t, repos.Projects.Upsert(ctx, storage.ProjectRecord{ID: "project", Name: "restart", RepoPath: dir, CreatedAt: at, UpdatedAt: at}))
	mustRestartOK(t, repos.Loops.Upsert(ctx, storage.LoopRecord{ID: loopID, Seq: 1, ProjectID: "project", Type: "reviewer", TargetType: "pull_request", TargetID: stringPtr("pr:acme/test:1"), Repo: stringPtr("acme/test"), PRNumber: int64Ptr(1), Status: "running", CreatedAt: at, UpdatedAt: at}))
	mustRestartOK(t, repos.Runs.Upsert(ctx, storage.RunRecord{ID: runID, LoopID: loopID, Status: "running", CurrentStep: stringPtr("review"), LastCompletedStep: stringPtr("worktree"), CheckpointJSON: stringPtr(checkpoint), StartedAt: at, CreatedAt: at, UpdatedAt: at}))
	mustRestartOK(t, repos.Queue.Upsert(ctx, storage.QueueItemRecord{ID: queueID, ProjectID: stringPtr("project"), LoopID: stringPtr(loopID), Type: "reviewer", TargetType: "pull_request", TargetID: "pr:acme/test:1", Repo: stringPtr("acme/test"), PRNumber: int64Ptr(1), DedupeKey: "reviewer:acme/test:1", Priority: 2, Status: "running", AvailableAt: at, MaxAttempts: 3, ClaimedAt: stringPtr(at), ClaimedBy: stringPtr("old"), CreatedAt: at, UpdatedAt: at}))

	entered, unblock := make(chan struct{}), make(chan struct{})
	var enterOnce, unblockOnce sync.Once
	allowFinalize := func() { unblockOnce.Do(func() { close(unblock) }) }
	defer allowFinalize()
	executor := agent.New(agent.ExecutorOptions{Config: agent.ExecutorConfig{Vendor: config.AgentVendorOpenCode}, Repos: repos, Owner: first.activeExecutions, Now: func() time.Time {
		// The silent shim leaves this clock unused until after leader wait.
		if _, err := os.Stat(filepath.Join(dir, "first-ready")); err == nil {
			enterOnce.Do(func() { close(entered) })
			<-unblock
		}
		return now
	}})
	input := agent.RunInput{ExecutionID: executionID, LoopID: loopID, RunID: runID, ProjectID: "project", WorkingDirectory: dir, Prompt: "review", Timeout: 10 * time.Second, GracefulShutdown: 50 * time.Millisecond}
	operation, err := first.activeExecutions.AdmitOperation(ctx, OperationMeta{ClaimedBy: "old"})
	mustRestartOK(t, err)
	defer operation.Release()
	claimed, err := repos.Queue.GetByID(ctx, queueID)
	mustRestartOK(t, err)
	permit, err := operation.BindClaim(*claimed)
	mustRestartOK(t, err)
	if !permit.Valid() {
		t.Fatal("operation did not bind the queue claim")
	}
	firstExecution, err := executor.Start(operation.Context(), input)
	mustRestartOK(t, err)
	roleDone := make(chan error, 1)
	go func() {
		defer operation.Release()
		_, err := firstExecution.Wait(operation.Context())
		if errors.Is(err, context.Canceled) {
			err = finalizeCancelledClaim(ctx, *claimed, defaultSchedulerTickInput{Repos: repos}, func() time.Time { return now })
		}
		roleDone <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "first-ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("contained child did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	row, err := repos.AgentExecutions.GetByID(ctx, executionID)
	mustRestartOK(t, err)
	if row == nil || row.PID == nil || syscall.Kill(-int(*row.PID), 0) != nil {
		t.Fatalf("missing live process group for %#v", row)
	}
	stopped := make(chan struct{})
	go func() { first.Stop("SIGTERM"); close(stopped) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("agent did not reach finalization")
	}
	select {
	case err := <-roleDone:
		mustRestartOK(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled role did not finalize its claim")
	}
	select {
	case <-stopped:
		t.Fatal("runtime stopped with finalization still blocked")
	case <-time.After(150 * time.Millisecond):
	}
	allowFinalize()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("runtime did not stop after finalization")
	}
	mustRestartOK(t, first.ShutdownDrainError())
	if err := syscall.Kill(int(*row.PID), 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("original leader was not reaped: %v", err)
	}

	second := start()
	repos = second.Services().Repositories
	row, err = repos.AgentExecutions.GetByID(ctx, executionID)
	mustRestartOK(t, err)
	if row == nil || row.Status != "killed" || row.EndedAt == nil {
		t.Fatalf("restart did not see terminal execution: %#v", row)
	}
	run, err := repos.Runs.GetByID(ctx, runID)
	mustRestartOK(t, err)
	if run == nil || run.Status != "interrupted" || derefString(run.CheckpointJSON) != checkpoint || derefString(run.LastCompletedStep) != "worktree" {
		t.Fatalf("unexpected recovery checkpoint or state: %#v", run)
	}
	summary, err := second.runRecoveryPipeline(ctx, repos, nil, now)
	mustRestartOK(t, err)
	if summary.InterruptedRunsMarked != 0 || summary.LoopsRequeued != 0 {
		t.Fatalf("second recovery repeated interruption/requeue: %#v", summary)
	}
	item, err := repos.Queue.ClaimNext(ctx, at, "new")
	mustRestartOK(t, err)
	if item == nil || item.ID != queueID {
		t.Fatalf("expected original claim, got %#v", item)
	}
	duplicate, err := repos.Queue.ClaimNext(ctx, at, "duplicate")
	mustRestartOK(t, err)
	if duplicate != nil {
		t.Fatalf("duplicate claim after restart: %#v", duplicate)
	}
	resumed := agent.New(agent.ExecutorOptions{Config: agent.ExecutorConfig{Vendor: config.AgentVendorOpenCode}, Repos: repos, Owner: second.activeExecutions})
	input.ExecutionID = "resumed"
	input.RunID = "resumed-run"
	mustRestartOK(t, repos.Runs.Upsert(ctx, storage.RunRecord{ID: input.RunID, LoopID: loopID, Status: "running", CurrentStep: run.CurrentStep, LastCompletedStep: run.LastCompletedStep, CheckpointJSON: run.CheckpointJSON, StartedAt: at, CreatedAt: at, UpdatedAt: at}))
	resumedExecution, err := resumed.Start(ctx, input)
	mustRestartOK(t, err)
	result, err := resumedExecution.Wait(ctx)
	mustRestartOK(t, err)
	if result.Status != "completed" {
		t.Fatalf("resumed agent status = %s", result.Status)
	}
	spawns, err := os.ReadFile(filepath.Join(dir, "spawns"))
	mustRestartOK(t, err)
	if strings.Count(string(spawns), "spawned\n") != 2 {
		t.Fatalf("expected one original spawn and one resumed spawn, got %q", spawns)
	}
}

func mustRestartOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
