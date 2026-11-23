package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nexu-io/looper/internal/config"
	"github.com/nexu-io/looper/internal/delivery"
	githubinfra "github.com/nexu-io/looper/internal/infra/github"
	"github.com/nexu-io/looper/internal/storage"
)

const (
	deliveryTestRepo     = "acme/looper"
	deliveryTestProject  = "demo"
	deliveryTestIssue    = int64(42)
	deliveryTestSpecPR   = int64(101)
	deliveryTestImplPR   = int64(102)
	deliveryTestSpecPath = "specs/2026-05-14-42-greeting.md"
	deliveryTestBranch   = "looper/planner/42-greeting"
)

var (
	deliveryHeadA  = strings.Repeat("a", 40)
	deliveryHeadB  = strings.Repeat("b", 40)
	deliveryMerge  = strings.Repeat("c", 40)
	deliveryMerge2 = strings.Repeat("d", 40)
)

// fakeDeliveryGitHub is a stateful hosting fake: label and close writes change
// the issue it later reads back, so lifecycle tests observe real effects.
type fakeDeliveryGitHub struct {
	GitHubGateway // nil: any unexpected call panics, which fails the test.

	issues        map[int64]*githubinfra.IssueDetail
	prs           map[int64]delivery.PullRequest
	specInPR      map[int64]bool
	specOnBase    map[string]bool
	timeline      map[int64][]map[string]any
	closeErr      []error
	removeErr     []error
	addErr        []error
	prErr         map[int64]error
	closeCalls    []int64
	addCalls      []githubinfra.IssueLabelsInput
	removeCalls   []githubinfra.IssueLabelsInput
	prReads       []int64
	timelineReads int
	issueReads    int
}

func newFakeDeliveryGitHub() *fakeDeliveryGitHub {
	return &fakeDeliveryGitHub{
		issues:     map[int64]*githubinfra.IssueDetail{},
		prs:        map[int64]delivery.PullRequest{},
		specInPR:   map[int64]bool{},
		specOnBase: map[string]bool{},
		timeline:   map[int64][]map[string]any{},
		prErr:      map[int64]error{},
	}
}

func (f *fakeDeliveryGitHub) ViewIssue(_ context.Context, input githubinfra.ViewIssueInput) (githubinfra.IssueDetail, error) {
	f.issueReads++
	issue, ok := f.issues[input.IssueNumber]
	if !ok {
		return githubinfra.IssueDetail{}, fmt.Errorf("issue %d not found", input.IssueNumber)
	}
	copy := *issue
	copy.Labels = append([]string(nil), issue.Labels...)
	return copy, nil
}

func (f *fakeDeliveryGitHub) AddIssueLabels(_ context.Context, input githubinfra.IssueLabelsInput) error {
	if len(f.addErr) > 0 {
		err := f.addErr[0]
		f.addErr = f.addErr[1:]
		if err != nil {
			return err
		}
	}
	f.addCalls = append(f.addCalls, input)
	issue := f.issues[input.IssueNumber]
	for _, label := range input.Labels {
		if !slices.Contains(issue.Labels, label) {
			issue.Labels = append(issue.Labels, label)
		}
	}
	return nil
}

func (f *fakeDeliveryGitHub) RemoveIssueLabels(_ context.Context, input githubinfra.IssueLabelsInput) error {
	if len(f.removeErr) > 0 {
		err := f.removeErr[0]
		f.removeErr = f.removeErr[1:]
		if err != nil {
			return err
		}
	}
	f.removeCalls = append(f.removeCalls, input)
	issue := f.issues[input.IssueNumber]
	issue.Labels = slices.DeleteFunc(issue.Labels, func(label string) bool { return slices.Contains(input.Labels, label) })
	return nil
}

func (f *fakeDeliveryGitHub) ListIssueTimeline(_ context.Context, input githubinfra.IssueTimelineInput) ([]map[string]any, error) {
	f.timelineReads++
	return f.timeline[input.IssueNumber], nil
}

func (f *fakeDeliveryGitHub) DeliveryPullRequest(_ context.Context, repo string, pr int64, _ string) (delivery.PullRequest, error) {
	f.prReads = append(f.prReads, pr)
	if err := f.prErr[pr]; err != nil {
		return delivery.PullRequest{}, err
	}
	value, ok := f.prs[pr]
	if !ok {
		return delivery.PullRequest{}, fmt.Errorf("pull request %d not found", pr)
	}
	return value, nil
}

func (f *fakeDeliveryGitHub) DeliverySpecOnBase(_ context.Context, _ string, base, path, merged, _ string) (bool, error) {
	return f.specOnBase[base+"|"+path+"|"+merged], nil
}

func (f *fakeDeliveryGitHub) DeliverySpecInPullRequest(_ context.Context, _ string, pr int64, path, _ string) (bool, error) {
	return f.specInPR[pr] && path == deliveryTestSpecPath, nil
}

func (f *fakeDeliveryGitHub) CloseDeliveredIssue(_ context.Context, _ string, issue int64, _ string) error {
	if len(f.closeErr) > 0 {
		err := f.closeErr[0]
		f.closeErr = f.closeErr[1:]
		if err != nil {
			return err
		}
	}
	f.closeCalls = append(f.closeCalls, issue)
	f.issues[issue].State = "closed"
	f.issues[issue].StateReason = "completed"
	return nil
}

type deliveryFixture struct {
	t      *testing.T
	ctx    context.Context
	repos  *storage.Repositories
	gh     *fakeDeliveryGitHub
	cfg    *config.Config
	now    time.Time
	runner *Runner

	runClock int
}

func newDeliveryFixture(t *testing.T) *deliveryFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, time.May, 14, 12, 0, 0, 0, time.UTC)
	coord, err := storage.OpenSQLiteCoordinator(ctx, filepath.Join(t.TempDir(), "delivery.sqlite"), storage.SQLiteCoordinatorOptions{Migrations: storage.EmbeddedMigrations})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coord.Close() })
	if _, err := coord.MigrationRunner().RunPending(ctx); err != nil {
		t.Fatal(err)
	}
	repos := storage.NewRepositories(coord.DB())
	base := "main"
	if err := repos.Projects.Upsert(ctx, storage.ProjectRecord{ID: deliveryTestProject, Name: "Demo", RepoPath: t.TempDir(), BaseBranch: &base, CreatedAt: now.Format(time.RFC3339), UpdatedAt: now.Format(time.RFC3339)}); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.DefaultConfig(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Roles.Worker.AutoDiscovery = true
	cfg.Roles.Reviewer.Behavior.ReviewEvents.AutoImplement = true
	gh := newFakeDeliveryGitHub()
	gh.issues[deliveryTestIssue] = &githubinfra.IssueDetail{Number: deliveryTestIssue, State: "open", Labels: []string{"triaged", "dispatch/plan", "looper:plan"}}
	f := &deliveryFixture{t: t, ctx: ctx, repos: repos, gh: gh, cfg: &cfg, now: now}
	f.restart()
	return f
}

// restart drops every in-memory coordinator state, as a daemon restart would.
func (f *deliveryFixture) restart() {
	f.runner = New(Options{Repos: f.repos, GitHub: f.gh, Config: f.cfg, Now: func() time.Time { return f.now }})
}

func (f *deliveryFixture) reconcile() error {
	f.t.Helper()
	return f.runner.ReconcileDeliveries(f.ctx, DiscoveryInput{ProjectID: deliveryTestProject, Repo: deliveryTestRepo})
}

func (f *deliveryFixture) mustReconcile() {
	f.t.Helper()
	if err := f.reconcile(); err != nil {
		f.t.Fatalf("ReconcileDeliveries() error = %v", err)
	}
}

func (f *deliveryFixture) insertLoop(loop storage.LoopRecord, metadata map[string]any) storage.LoopRecord {
	f.t.Helper()
	seq, err := f.repos.Loops.AllocateSeq(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	encoded, _ := json.Marshal(metadata)
	raw := string(encoded)
	loop.Seq = seq
	loop.MetadataJSON = &raw
	if loop.ProjectID == "" {
		loop.ProjectID = deliveryTestProject
	}
	if loop.Repo == nil {
		repo := deliveryTestRepo
		loop.Repo = &repo
	}
	loop.CreatedAt = f.now.Format(jsISOStringLayout)
	loop.UpdatedAt = loop.CreatedAt
	if err := f.repos.Loops.Upsert(f.ctx, loop); err != nil {
		f.t.Fatal(err)
	}
	return loop
}

func (f *deliveryFixture) insertRun(id, loopID string, checkpoint map[string]any) {
	f.t.Helper()
	encoded, _ := json.Marshal(checkpoint)
	raw := string(encoded)
	// Each run starts later than the previous one so newest-first is defined.
	f.runClock++
	at := f.now.Add(time.Duration(f.runClock) * time.Millisecond).Format(jsISOStringLayout)
	if err := f.repos.Runs.Upsert(f.ctx, storage.RunRecord{ID: id, LoopID: loopID, Status: "success", CheckpointJSON: &raw, StartedAt: at, CreatedAt: at, UpdatedAt: at}); err != nil {
		f.t.Fatal(err)
	}
}

func int64Ptr(value int64) *int64 { return &value }
func strPtr(value string) *string { return &value }

func (f *deliveryFixture) seedPlanner(id string, issue, pr int64) storage.LoopRecord {
	f.t.Helper()
	loop := f.insertLoop(storage.LoopRecord{ID: id, Type: "planner", TargetType: "issue", TargetID: strPtr(fmt.Sprintf("issue:%s:%d", deliveryTestRepo, issue)), PRNumber: int64Ptr(pr), Status: "completed"},
		map[string]any{"issueNumber": issue, "specPath": deliveryTestSpecPath, "branch": deliveryTestBranch, "prNumber": pr})
	f.insertRun(id+"-run", id, map[string]any{
		"issue":    map[string]any{"repo": deliveryTestRepo, "issueNumber": issue, "specPath": deliveryTestSpecPath},
		"worktree": map[string]any{"branch": deliveryTestBranch, "baseBranch": "main", "specPath": deliveryTestSpecPath},
		"publish":  map[string]any{"pushed": true, "pullRequest": map[string]any{"number": pr}},
	})
	return loop
}

func (f *deliveryFixture) mergeSpec(pr int64, merge string) {
	f.gh.prs[pr] = delivery.PullRequest{Number: pr, State: delivery.PullRequestMerged, BaseBranch: "main", HeadSHA: deliveryHeadA, MergeSHA: merge, Repo: deliveryTestRepo, HeadBranch: deliveryTestBranch, BaseContainsMerge: true}
	f.gh.specInPR[pr] = true
	f.gh.specOnBase["main|"+deliveryTestSpecPath+"|"+merge] = true
}

func deliveryRecord(loopID, runID, validationRun string, complete bool) delivery.Record {
	return delivery.Record{ProjectID: deliveryTestProject, Repo: deliveryTestRepo, WorkerLoopID: loopID, RunID: runID, IssueNumber: deliveryTestIssue, PRNumber: deliveryTestImplPR, BaseBranch: "main", HeadSHA: deliveryHeadB, SpecPath: deliveryTestSpecPath, Commits: []string{deliveryHeadB}, ValidationRef: validationRun + ":validate", Published: true, Complete: complete}
}

// seedWorker records a published delivery whose validation ran in run A and
// whose publication completed in a resumed run B.
func (f *deliveryFixture) seedWorker(id, status string, record delivery.Record) storage.LoopRecord {
	f.t.Helper()
	loop := f.insertLoop(storage.LoopRecord{ID: id, Type: "worker", TargetType: "pull_request", TargetID: strPtr(fmt.Sprintf("pr:%s:%d", deliveryTestRepo, record.PRNumber)), PRNumber: int64Ptr(record.PRNumber), Status: status},
		map[string]any{"worker": map[string]any{"issueNumber": record.IssueNumber, "repo": deliveryTestRepo}, "branch": "looper/worker/42", implementationDeliveryKey: record})
	validation := map[string]any{"runId": id + "-A", "commands": []string{"go test ./..."}, "passed": true, "validatedHead": deliveryHeadB}
	f.insertRun(id+"-A", id, map[string]any{"validation": validation})
	f.insertRun(id+"-B", id, map[string]any{"validation": validation, "implementationDelivery": record})
	return loop
}

func (f *deliveryFixture) openImpl() {
	f.gh.prs[deliveryTestImplPR] = delivery.PullRequest{Number: deliveryTestImplPR, State: delivery.PullRequestOpen, BaseBranch: "main", HeadSHA: deliveryHeadB, Repo: deliveryTestRepo, HeadBranch: "looper/worker/42"}
}

func (f *deliveryFixture) mergeImpl(head, merge string) {
	f.gh.prs[deliveryTestImplPR] = delivery.PullRequest{Number: deliveryTestImplPR, State: delivery.PullRequestMerged, BaseBranch: "main", HeadSHA: head, MergeSHA: merge, Repo: deliveryTestRepo, HeadBranch: "looper/worker/42", BaseContainsMerge: true}
}

func (f *deliveryFixture) loopMetadata(id string) map[string]any {
	f.t.Helper()
	loop, err := f.repos.Loops.GetByID(f.ctx, id)
	if err != nil || loop == nil {
		f.t.Fatalf("GetByID(%s) = %v, %v", id, loop, err)
	}
	return deliveryMetadataObject(loop.MetadataJSON)
}

func (f *deliveryFixture) reconcileState(id string) deliveryReconcileState {
	return readDeliveryReconcileState(f.loopMetadata(id))
}

func (f *deliveryFixture) mergedState(id string) *implementationMergedState {
	return readImplementationMergedState(f.loopMetadata(id))
}

func (f *deliveryFixture) issueLabels() []string {
	return append([]string(nil), f.gh.issues[deliveryTestIssue].Labels...)
}

func TestReconcileDeliveriesLifecycleAcrossRestart(t *testing.T) {
	for _, tc := range []struct {
		name  string
		merge string
	}{
		{name: "merge commit", merge: deliveryMerge},
		{name: "squash", merge: deliveryMerge2},
		{name: "rebase fast-forward", merge: deliveryHeadB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			f.gh.prs[deliveryTestSpecPR] = delivery.PullRequest{Number: deliveryTestSpecPR, State: delivery.PullRequestOpen, BaseBranch: "main", HeadSHA: deliveryHeadA, Repo: deliveryTestRepo, HeadBranch: deliveryTestBranch}

			f.mustReconcile()
			if len(f.gh.addCalls) != 0 || f.reconcileState("planner-1").Done {
				t.Fatalf("open spec PR must not hand off: adds=%v state=%+v", f.gh.addCalls, f.reconcileState("planner-1"))
			}

			f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
			f.mustReconcile()
			labels := f.issueLabels()
			if !slices.Contains(labels, "looper:worker-ready") || !slices.Contains(labels, "dispatch/implement") || slices.Contains(labels, "dispatch/plan") || slices.Contains(labels, "looper:plan") {
				t.Fatalf("handoff labels = %v", labels)
			}
			if state := f.reconcileState("planner-1"); !state.Done || state.Outcome != "handed-off" || state.PRNumber != deliveryTestSpecPR {
				t.Fatalf("planner state = %+v", state)
			}

			// The Worker publishes and its PR is still open: nothing closes.
			f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
			f.openImpl()
			reads := len(f.gh.prReads)
			f.mustReconcile()
			if len(f.gh.closeCalls) != 0 || f.mergedState("worker-1") != nil {
				t.Fatalf("open implementation must not close: closes=%v", f.gh.closeCalls)
			}
			if got := slices.Index(f.gh.prReads[reads:], deliveryTestSpecPR); got >= 0 {
				t.Fatalf("finished planner loop was rescanned: %v", f.gh.prReads[reads:])
			}

			f.mergeImpl(deliveryHeadB, tc.merge)
			f.restart()
			f.mustReconcile()
			if len(f.gh.closeCalls) != 1 {
				t.Fatalf("close calls = %v", f.gh.closeCalls)
			}
			merged := f.mergedState("worker-1")
			if merged == nil || merged.MergeSHA != tc.merge || merged.PRNumber != deliveryTestImplPR || merged.Outcome != "closed" {
				t.Fatalf("implementationMerged = %+v", merged)
			}
			state := f.reconcileState("worker-1")
			if !state.Done || !state.IssueClosed || !state.LabelsReconciled || state.CloseRequestedAt == "" || state.ReconciledAt == "" {
				t.Fatalf("worker reconcile state = %+v", state)
			}
			if labels := f.issueLabels(); slices.Contains(labels, "looper:worker-ready") || slices.Contains(labels, "dispatch/implement") {
				t.Fatalf("labels after close = %v", labels)
			}

			// A human reopens the issue after delivery: it is never closed again.
			f.gh.issues[deliveryTestIssue].State = "open"
			f.gh.issues[deliveryTestIssue].StateReason = "reopened"
			for range 2 {
				f.restart()
				f.mustReconcile()
			}
			if len(f.gh.closeCalls) != 1 {
				t.Fatalf("reopened issue was closed again: %v", f.gh.closeCalls)
			}
		})
	}
}

func TestReconcileDeliveriesValidationReferenceMustResolveInLoop(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ref       string
		wantClose bool
	}{
		{name: "earlier validation run", ref: "worker-1-A", wantClose: true},
		{name: "publishing run without its own validation", ref: "worker-1-B", wantClose: false},
		{name: "foreign run", ref: "other-run", wantClose: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", tc.ref, true))
			f.mergeImpl(deliveryHeadB, deliveryMerge)
			f.mustReconcile()
			if got := len(f.gh.closeCalls) == 1; got != tc.wantClose {
				t.Fatalf("closed = %v, want %v (state=%+v)", got, tc.wantClose, f.reconcileState("worker-1"))
			}
			if !tc.wantClose && len(f.gh.prReads) != 0 {
				t.Fatalf("unverified validation must not read remote state: %v", f.gh.prReads)
			}
		})
	}
}

func TestReconcileDeliveriesPartialFailuresAreCompensated(t *testing.T) {
	t.Run("label removal fails before close", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.gh.issues[deliveryTestIssue].Labels = []string{"dispatch/implement", "looper:worker-ready"}
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.gh.removeErr = []error{errors.New("503 label service")}
		if err := f.reconcile(); err == nil {
			t.Fatal("expected label failure to surface")
		}
		if len(f.gh.closeCalls) != 0 || f.reconcileState("worker-1").Done {
			t.Fatalf("close must wait for labels: closes=%v", f.gh.closeCalls)
		}
		f.restart()
		f.mustReconcile()
		if len(f.gh.closeCalls) != 1 || !f.reconcileState("worker-1").Done {
			t.Fatalf("retry did not finish: closes=%v state=%+v", f.gh.closeCalls, f.reconcileState("worker-1"))
		}
	})
	t.Run("close fails after intent", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.gh.closeErr = []error{errors.New("502 bad gateway")}
		if err := f.reconcile(); err == nil {
			t.Fatal("expected close failure to surface")
		}
		state := f.reconcileState("worker-1")
		if state.Done || state.CloseRequestedAt == "" || f.mergedState("worker-1") == nil {
			t.Fatalf("intent and merge fact must be durable: %+v merged=%+v", state, f.mergedState("worker-1"))
		}
		f.restart()
		f.mustReconcile()
		if len(f.gh.closeCalls) != 1 || !f.reconcileState("worker-1").IssueClosed {
			t.Fatalf("retry did not close exactly once: %v", f.gh.closeCalls)
		}
		if f.gh.timelineReads != 1 {
			t.Fatalf("reopen guard must consult timeline once, got %d", f.gh.timelineReads)
		}
	})
	t.Run("crash after close before final marker", func(t *testing.T) {
		f := newDeliveryFixture(t)
		loop := f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.writeIntent(loop.ID, f.now.Add(-time.Minute))
		f.gh.issues[deliveryTestIssue].State, f.gh.issues[deliveryTestIssue].StateReason = "closed", "completed"
		f.mustReconcile()
		if state := f.reconcileState("worker-1"); len(f.gh.closeCalls) != 0 || !state.Done || !state.IssueClosed {
			t.Fatalf("recovered state = %+v closes=%v", state, f.gh.closeCalls)
		}
	})
	t.Run("human reopened between close and marker", func(t *testing.T) {
		f := newDeliveryFixture(t)
		loop := f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.writeIntent(loop.ID, f.now.Add(-time.Minute))
		f.gh.timeline[deliveryTestIssue] = []map[string]any{{"event": "closed", "created_at": f.now.Add(-50 * time.Second).Format(time.RFC3339)}, {"event": "reopened", "created_at": f.now.Add(-10 * time.Second).Format(time.RFC3339)}}
		f.mustReconcile()
		if state := f.reconcileState("worker-1"); len(f.gh.closeCalls) != 0 || !state.Done || state.Reason != string(deliveryCloseReopened) || state.IssueClosed {
			t.Fatalf("reopened state = %+v closes=%v", state, f.gh.closeCalls)
		}
	})
	t.Run("handoff label add fails", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
		f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
		f.gh.addErr = []error{errors.New("timeout")}
		if err := f.reconcile(); err == nil {
			t.Fatal("expected add failure")
		}
		if labels := f.issueLabels(); !slices.Contains(labels, "looper:plan") || f.reconcileState("planner-1").Done {
			t.Fatalf("planning labels must survive a failed handoff: %v", labels)
		}
		f.mustReconcile()
		if labels := f.issueLabels(); !slices.Contains(labels, "looper:worker-ready") || slices.Contains(labels, "looper:plan") || !f.reconcileState("planner-1").Done {
			t.Fatalf("retry handoff labels = %v", labels)
		}
	})
}

func (f *deliveryFixture) writeIntent(loopID string, at time.Time) {
	f.t.Helper()
	metadata := f.loopMetadata(loopID)
	metadata[deliveryReconcileMetadataKey] = deliveryReconcileState{Kind: "implementation", PRNumber: deliveryTestImplPR, IssueNumber: deliveryTestIssue, CloseRequestedAt: at.UTC().Format(jsISOStringLayout), LabelsReconciled: true}
	encoded, _ := json.Marshal(metadata)
	raw := string(encoded)
	loop, _ := f.repos.Loops.GetByID(f.ctx, loopID)
	if ok, err := f.repos.Loops.UpdateMetadataIfMatch(f.ctx, loopID, &raw, f.now.Format(jsISOStringLayout), loop.MetadataJSON); err != nil || !ok {
		f.t.Fatalf("write intent: %v %v", ok, err)
	}
}

func TestReconcileDeliveriesImplementationVetoes(t *testing.T) {
	type expectation struct {
		done        bool
		merged      bool
		reason      string
		issueState  string
		labelsClean bool
	}
	for _, tc := range []struct {
		name   string
		setup  func(*deliveryFixture)
		record func(delivery.Record) delivery.Record
		status string
		want   expectation
	}{
		{name: "issue hold", setup: func(f *deliveryFixture) {
			f.gh.issues[deliveryTestIssue].Labels = append(f.gh.issues[deliveryTestIssue].Labels, "looper:hold")
		}, want: expectation{merged: true, reason: "held-or-in-flight"}},
		{name: "worker lane hold", setup: func(f *deliveryFixture) {
			f.gh.issues[deliveryTestIssue].Labels = append(f.gh.issues[deliveryTestIssue].Labels, "looper:hold:worker")
		}, want: expectation{merged: true, reason: "held-or-in-flight"}},
		{name: "reviewer running on implementation PR", setup: func(f *deliveryFixture) {
			f.insertLoop(storage.LoopRecord{ID: "reviewer-1", Type: "reviewer", TargetType: "pull_request", TargetID: strPtr("pr"), PRNumber: int64Ptr(deliveryTestImplPR), Status: "running"}, map[string]any{})
		}, want: expectation{merged: true, reason: "held-or-in-flight"}},
		{name: "fixer scope hold", setup: func(f *deliveryFixture) {
			f.insertLoop(storage.LoopRecord{ID: "fixer-1", Type: "fixer", TargetType: "pull_request", TargetID: strPtr("pr"), PRNumber: int64Ptr(deliveryTestImplPR), Status: "paused"}, map[string]any{"reviewScopeHuman": map[string]any{"heldBy": "reviewer"}})
		}, want: expectation{merged: true, reason: "held-or-in-flight"}},
		{name: "reviewer pending scope evidence", setup: func(f *deliveryFixture) {
			f.insertLoop(storage.LoopRecord{ID: "reviewer-1", Type: "reviewer", TargetType: "pull_request", TargetID: strPtr("pr"), PRNumber: int64Ptr(deliveryTestImplPR), Status: "waiting"}, map[string]any{"reviewScopeHuman": map[string]any{"pending": true}})
		}, want: expectation{merged: true, reason: "held-or-in-flight"}},
		{name: "another active worker on issue", setup: func(f *deliveryFixture) {
			f.insertLoop(storage.LoopRecord{ID: "worker-2", Type: "worker", TargetType: "issue", TargetID: strPtr(fmt.Sprintf("issue:%s:%d", deliveryTestRepo, deliveryTestIssue)), Status: "queued"}, map[string]any{})
		}, want: expectation{merged: true, reason: "held-or-in-flight"}},
		{name: "delivering worker still running", status: "running", want: expectation{merged: true, reason: "held-or-in-flight"}},
		{name: "partial delivery", record: func(r delivery.Record) delivery.Record { r.Complete = false; return r }, want: expectation{merged: true, reason: "partial-delivery"}},
		{name: "metadata pending", record: func(r delivery.Record) delivery.Record { r.MetadataPending = true; return r }, want: expectation{}},
		{name: "unpublished", record: func(r delivery.Record) delivery.Record { r.Published = false; return r }, want: expectation{}},
		{name: "wrong issue association", record: func(r delivery.Record) delivery.Record { r.ProjectID = "other"; return r }, want: expectation{done: true, reason: "unverified-delivery-binding"}},
		{name: "wrong worker loop", record: func(r delivery.Record) delivery.Record { r.WorkerLoopID = "someone-else"; return r }, want: expectation{done: true, reason: "unverified-delivery-binding"}},
		{name: "head moved after publication", setup: func(f *deliveryFixture) { f.mergeImpl(deliveryHeadA, deliveryMerge) }, want: expectation{done: true, merged: true, reason: string(deliveryCloseHeadChanged)}},
		{name: "different base", setup: func(f *deliveryFixture) {
			pr := f.gh.prs[deliveryTestImplPR]
			pr.BaseBranch = "release"
			f.gh.prs[deliveryTestImplPR] = pr
		}, want: expectation{done: true, reason: "base-mismatch"}},
		{name: "fork or foreign source", setup: func(f *deliveryFixture) {
			pr := f.gh.prs[deliveryTestImplPR]
			pr.Repo = "fork/looper"
			f.gh.prs[deliveryTestImplPR] = pr
		}, want: expectation{done: true, reason: "misassociated-pull-request"}},
		{name: "merge not on target", setup: func(f *deliveryFixture) {
			pr := f.gh.prs[deliveryTestImplPR]
			pr.BaseContainsMerge = false
			f.gh.prs[deliveryTestImplPR] = pr
		}, want: expectation{}},
		{name: "closed unmerged", setup: func(f *deliveryFixture) {
			f.gh.prs[deliveryTestImplPR] = delivery.PullRequest{Number: deliveryTestImplPR, State: delivery.PullRequestClosed, BaseBranch: "main", HeadSHA: deliveryHeadB, Repo: deliveryTestRepo}
		}, want: expectation{done: true, reason: "pull-request-closed-unmerged"}},
		{name: "issue closed not planned", setup: func(f *deliveryFixture) {
			f.gh.issues[deliveryTestIssue].State, f.gh.issues[deliveryTestIssue].StateReason = "closed", "not_planned"
		}, want: expectation{done: true, merged: true, reason: string(deliveryCloseNotPlanned), issueState: "not_planned", labelsClean: true}},
		{name: "issue already closed by human", setup: func(f *deliveryFixture) {
			f.gh.issues[deliveryTestIssue].State, f.gh.issues[deliveryTestIssue].StateReason = "closed", "completed"
		}, want: expectation{done: true, merged: true, reason: string(deliveryCloseIssueClosed), issueState: "completed", labelsClean: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.gh.issues[deliveryTestIssue].Labels = []string{"triaged", "dispatch/implement", "looper:worker-ready"}
			record := deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true)
			if tc.record != nil {
				record = tc.record(record)
			}
			status := tc.status
			if status == "" {
				status = "completed"
			}
			f.seedWorker("worker-1", status, record)
			f.mergeImpl(deliveryHeadB, deliveryMerge)
			if tc.setup != nil {
				tc.setup(f)
			}
			labelsBefore := f.issueLabels()
			f.mustReconcile()
			if len(f.gh.closeCalls) != 0 {
				t.Fatalf("veto closed the issue: %v", f.gh.closeCalls)
			}
			if !tc.want.labelsClean {
				if len(f.gh.removeCalls) != 0 || !slices.Equal(labelsBefore, f.issueLabels()) {
					t.Fatalf("veto changed labels: before=%v after=%v", labelsBefore, f.issueLabels())
				}
			} else if slices.Contains(f.issueLabels(), "looper:worker-ready") || slices.Contains(f.issueLabels(), "dispatch/implement") {
				t.Fatalf("stale stage labels on a finished issue: %v", f.issueLabels())
			}
			state := f.reconcileState("worker-1")
			if state.Done != tc.want.done || state.Reason != tc.want.reason || state.IssueClosed {
				t.Fatalf("state = %+v, want %+v", state, tc.want)
			}
			if got := f.mergedState("worker-1") != nil; got != tc.want.merged {
				t.Fatalf("implementationMerged present = %v, want %v", got, tc.want.merged)
			}
			if tc.want.issueState != "" && f.gh.issues[deliveryTestIssue].StateReason != tc.want.issueState {
				t.Fatalf("issue reason changed to %q", f.gh.issues[deliveryTestIssue].StateReason)
			}
			if tc.want.reason == "held-or-in-flight" {
				// The deferral is stable: repeated ticks do not rewrite the row.
				before, _ := f.repos.Loops.GetByID(f.ctx, "worker-1")
				f.now = f.now.Add(time.Minute)
				f.mustReconcile()
				after, _ := f.repos.Loops.GetByID(f.ctx, "worker-1")
				if deliveryDerefString(before.MetadataJSON) != deliveryDerefString(after.MetadataJSON) {
					t.Fatalf("deferred candidate churned metadata")
				}
			}
		})
	}
}

func TestReconcileDeliveriesHoldReleaseClosesLater(t *testing.T) {
	f := newDeliveryFixture(t)
	f.gh.issues[deliveryTestIssue].Labels = []string{"looper:hold", "looper:worker-ready"}
	f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
	f.mergeImpl(deliveryHeadB, deliveryMerge)
	f.mustReconcile()
	if len(f.gh.closeCalls) != 0 || f.mergedState("worker-1") == nil {
		t.Fatalf("held issue: closes=%v merged=%+v", f.gh.closeCalls, f.mergedState("worker-1"))
	}
	f.gh.issues[deliveryTestIssue].Labels = []string{"looper:worker-ready"}
	f.mustReconcile()
	if len(f.gh.closeCalls) != 1 {
		t.Fatalf("released hold did not close: %v", f.gh.closeCalls)
	}
}

func TestReconcileDeliveriesPlanningVetoes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		setup    func(*deliveryFixture)
		wantDone bool
		wantAdds bool
		reason   string
	}{
		{name: "metadata without planner run is not authority", setup: func(f *deliveryFixture) {
			f.insertLoop(storage.LoopRecord{ID: "planner-1", Type: "planner", TargetType: "issue", TargetID: strPtr(fmt.Sprintf("issue:%s:%d", deliveryTestRepo, deliveryTestIssue)), PRNumber: int64Ptr(deliveryTestSpecPR), Status: "completed"},
				map[string]any{"issueNumber": deliveryTestIssue, "specPath": deliveryTestSpecPath, "prNumber": deliveryTestSpecPR})
		}},
		{name: "checkpoint published another PR", setup: func(f *deliveryFixture) {
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			f.insertRun("planner-1-run-2", "planner-1", map[string]any{"issue": map[string]any{"issueNumber": deliveryTestIssue, "specPath": deliveryTestSpecPath}, "publish": map[string]any{"pullRequest": map[string]any{"number": 999}}})
		}, wantDone: true, reason: "conflicting-planner-checkpoint"},
		{name: "row PR disagrees with metadata", setup: func(f *deliveryFixture) {
			f.insertLoop(storage.LoopRecord{ID: "planner-1", Type: "planner", TargetType: "issue", TargetID: strPtr(fmt.Sprintf("issue:%s:%d", deliveryTestRepo, deliveryTestIssue)), PRNumber: int64Ptr(deliveryTestSpecPR), Status: "completed"},
				map[string]any{"issueNumber": deliveryTestIssue, "specPath": deliveryTestSpecPath, "prNumber": 7})
		}, wantDone: true, reason: "unverified-planner-evidence"},
		{name: "PR did not add the spec", setup: func(f *deliveryFixture) {
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			f.gh.specInPR[deliveryTestSpecPR] = false
		}, wantDone: true, reason: "spec-not-in-pull-request"},
		{name: "spec missing on base", setup: func(f *deliveryFixture) {
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			f.gh.specOnBase = map[string]bool{}
		}, wantDone: true, reason: "spec-not-on-base"},
		{name: "head branch differs", setup: func(f *deliveryFixture) {
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			pr := f.gh.prs[deliveryTestSpecPR]
			pr.HeadBranch = "human/branch"
			f.gh.prs[deliveryTestSpecPR] = pr
		}, wantDone: true, reason: "misassociated-spec-pr"},
		{name: "issue held", setup: func(f *deliveryFixture) {
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			f.gh.issues[deliveryTestIssue].Labels = append(f.gh.issues[deliveryTestIssue].Labels, "looper:hold")
		}},
		{name: "reviewer holds spec scope", setup: func(f *deliveryFixture) {
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			f.insertLoop(storage.LoopRecord{ID: "reviewer-1", Type: "reviewer", TargetType: "pull_request", TargetID: strPtr("pr"), PRNumber: int64Ptr(deliveryTestSpecPR), Status: "awaiting_human"}, map[string]any{})
		}},
		{name: "autoImplement disabled", setup: func(f *deliveryFixture) {
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			f.cfg.Roles.Reviewer.Behavior.ReviewEvents.AutoImplement = false
		}},
		{name: "historical undelivered worker still gets handoff", setup: func(f *deliveryFixture) {
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			f.insertLoop(storage.LoopRecord{ID: "worker-1", Type: "worker", TargetType: "issue", TargetID: strPtr(fmt.Sprintf("issue:%s:%d", deliveryTestRepo, deliveryTestIssue)), Status: "failed"}, map[string]any{})
		}, wantDone: true, wantAdds: true},
		{name: "issue closed", setup: func(f *deliveryFixture) {
			f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
			f.gh.issues[deliveryTestIssue].State, f.gh.issues[deliveryTestIssue].StateReason = "closed", "not_planned"
		}, wantDone: true, reason: "issue-not-open"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
			tc.setup(f)
			before := f.issueLabels()
			f.mustReconcile()
			if !tc.wantAdds {
				if len(f.gh.addCalls) != 0 || len(f.gh.removeCalls) != 0 || !slices.Equal(before, f.issueLabels()) {
					t.Fatalf("veto changed labels: adds=%v removes=%v", f.gh.addCalls, f.gh.removeCalls)
				}
			} else {
				labels := f.issueLabels()
				if !slices.Contains(labels, "looper:worker-ready") || slices.Contains(labels, "looper:plan") || slices.Contains(labels, "dispatch/plan") {
					t.Fatalf("undelivered worker must not block handoff: %v", labels)
				}
			}
			if f.gh.issues[deliveryTestIssue].State == "closed" && f.gh.issues[deliveryTestIssue].StateReason != "not_planned" && tc.name == "issue closed" {
				t.Fatal("issue reason changed")
			}
			state := f.reconcileState("planner-1")
			if state.Done != tc.wantDone || state.Reason != tc.reason {
				t.Fatalf("state = %+v, want done=%v reason=%q", state, tc.wantDone, tc.reason)
			}
		})
	}
}

func TestReconcileDeliveriesScanIsBoundedAndFair(t *testing.T) {
	f := newDeliveryFixture(t)
	const total = 25
	for i := 0; i < total; i++ {
		issue := int64(1000 + i)
		pr := int64(2000 + i)
		f.seedPlanner(fmt.Sprintf("planner-%02d", i), issue, pr)
		f.gh.prs[pr] = delivery.PullRequest{Number: pr, State: delivery.PullRequestOpen, BaseBranch: "main", Repo: deliveryTestRepo, HeadBranch: deliveryTestBranch}
	}
	// Unrelated terminal history must not consume scan budget.
	for i := 0; i < 50; i++ {
		f.insertLoop(storage.LoopRecord{ID: fmt.Sprintf("noise-%02d", i), Type: "worker", TargetType: "issue", TargetID: strPtr("noise"), Status: "completed"}, map[string]any{"worker": map[string]any{}})
	}
	seen := map[int64]int{}
	ticks := 0
	for len(seen) < total {
		ticks++
		if ticks > 20 {
			t.Fatalf("scan starved candidates: saw %d of %d", len(seen), total)
		}
		before := len(f.gh.prReads)
		f.mustReconcile()
		reads := f.gh.prReads[before:]
		if len(reads) > deliveryPlanningCandidatesPerPass {
			t.Fatalf("tick %d read %d PRs, bound is %d", ticks, len(reads), deliveryPlanningCandidatesPerPass)
		}
		for _, pr := range reads {
			seen[pr]++
		}
	}
	if want := (total + deliveryPlanningCandidatesPerPass - 1) / deliveryPlanningCandidatesPerPass; ticks > want+1 {
		t.Fatalf("fair rotation took %d ticks, want <= %d", ticks, want+1)
	}
}

func TestReconcileDeliveriesOneFailureDoesNotBlockOthers(t *testing.T) {
	f := newDeliveryFixture(t)
	f.seedPlanner("planner-bad", 7, 70)
	f.gh.prErr[70] = errors.New("500 server error")
	f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
	f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
	if err := f.reconcile(); err == nil || !strings.Contains(err.Error(), "planner-bad") {
		t.Fatalf("expected the failing candidate to be reported, got %v", err)
	}
	if !f.reconcileState("planner-1").Done || !slices.Contains(f.issueLabels(), "looper:worker-ready") {
		t.Fatalf("healthy candidate was blocked: %+v labels=%v", f.reconcileState("planner-1"), f.issueLabels())
	}
}

func TestReconcileDeliveriesProjectGates(t *testing.T) {
	t.Run("archived project", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		if _, err := f.repos.Projects.Archive(f.ctx, deliveryTestProject, f.now.Format(time.RFC3339)); err != nil {
			t.Fatal(err)
		}
		f.mustReconcile()
		if len(f.gh.prReads) != 0 || len(f.gh.closeCalls) != 0 {
			t.Fatalf("archived project touched remote state: reads=%v closes=%v", f.gh.prReads, f.gh.closeCalls)
		}
	})
	t.Run("routed project", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.cfg.Projects = []config.ProjectRefConfig{{ID: deliveryTestProject, Network: config.ProjectNetworkConfig{Mode: config.NetworkModeRouted}}}
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		if len(f.gh.prReads) != 0 {
			t.Fatalf("routed project touched remote state: %v", f.gh.prReads)
		}
	})
	t.Run("gateway without delivery support", func(t *testing.T) {
		f := newDeliveryFixture(t)
		runner := New(Options{Repos: f.repos, GitHub: struct{ GitHubGateway }{}, Config: f.cfg, Now: func() time.Time { return f.now }})
		if err := runner.ReconcileDeliveries(f.ctx, DiscoveryInput{ProjectID: deliveryTestProject, Repo: deliveryTestRepo}); err == nil {
			t.Fatal("expected unsupported gateway error")
		}
	})
}

func TestReconcileDeliveriesReplacementPRReopensCandidate(t *testing.T) {
	f := newDeliveryFixture(t)
	loop := f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
	f.gh.prs[deliveryTestSpecPR] = delivery.PullRequest{Number: deliveryTestSpecPR, State: delivery.PullRequestClosed, BaseBranch: "main", Repo: deliveryTestRepo, HeadBranch: deliveryTestBranch}
	f.mustReconcile()
	if state := f.reconcileState("planner-1"); !state.Done || state.Reason != "spec-pr-closed-unmerged" {
		t.Fatalf("closed spec state = %+v", state)
	}
	// The Planner republishes as PR 103.
	current, _ := f.repos.Loops.GetByID(f.ctx, loop.ID)
	metadata := deliveryMetadataObject(current.MetadataJSON)
	metadata["prNumber"] = 103
	encoded, _ := json.Marshal(metadata)
	current.MetadataJSON = strPtr(string(encoded))
	current.PRNumber = int64Ptr(103)
	if err := f.repos.Loops.Upsert(f.ctx, *current); err != nil {
		t.Fatal(err)
	}
	f.insertRun("planner-1-run-2", loop.ID, map[string]any{"issue": map[string]any{"issueNumber": deliveryTestIssue, "specPath": deliveryTestSpecPath}, "worktree": map[string]any{"branch": deliveryTestBranch, "baseBranch": "main"}, "publish": map[string]any{"pullRequest": map[string]any{"number": 103}}})
	// The newest publishing run supersedes the closed PR 101.
	f.mergeSpec(103, deliveryMerge)
	f.mustReconcile()
	if state := f.reconcileState("planner-1"); state.PRNumber != 103 || !state.Done || state.Outcome != "handed-off" {
		t.Fatalf("replacement PR state = %+v", state)
	}
	if !slices.Contains(f.issueLabels(), "looper:worker-ready") {
		t.Fatalf("replacement PR did not hand off: %v", f.issueLabels())
	}
}

func TestReconcileDeliveriesRepublishedHeadIsReconsidered(t *testing.T) {
	f := newDeliveryFixture(t)
	loop := f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "missing-run", true))
	f.mergeImpl(deliveryHeadA, deliveryMerge)
	f.mustReconcile()
	if state := f.reconcileState(loop.ID); !state.Done || state.Reason != "unverified-validation-reference" {
		t.Fatalf("first record state = %+v", state)
	}
	// Worker re-validates and re-publishes head A on the same PR.
	record := deliveryRecord(loop.ID, "worker-1-C", "worker-1-C", true)
	record.HeadSHA, record.Commits = deliveryHeadA, []string{deliveryHeadA}
	f.insertRun("worker-1-C", loop.ID, map[string]any{"validation": map[string]any{"runId": "worker-1-C", "commands": []string{"go test ./..."}, "passed": true, "validatedHead": deliveryHeadA}})
	current, _ := f.repos.Loops.GetByID(f.ctx, loop.ID)
	metadata := deliveryMetadataObject(current.MetadataJSON)
	metadata[implementationDeliveryKey] = record
	encoded, _ := json.Marshal(metadata)
	current.MetadataJSON = strPtr(string(encoded))
	if err := f.repos.Loops.Upsert(f.ctx, *current); err != nil {
		t.Fatal(err)
	}
	f.mustReconcile()
	if len(f.gh.closeCalls) != 1 || !f.reconcileState(loop.ID).IssueClosed {
		t.Fatalf("republished verified head was not reconsidered: closes=%v state=%+v", f.gh.closeCalls, f.reconcileState(loop.ID))
	}
}

func TestReconcileDeliveriesSkipsInvalidMetadataRows(t *testing.T) {
	f := newDeliveryFixture(t)
	seq, _ := f.repos.Loops.AllocateSeq(f.ctx)
	at := f.now.Format(jsISOStringLayout)
	if err := f.repos.Loops.Upsert(f.ctx, storage.LoopRecord{ID: "broken", Seq: seq, ProjectID: deliveryTestProject, Type: "planner", TargetType: "issue", Repo: strPtr(deliveryTestRepo), PRNumber: int64Ptr(5), Status: "completed", MetadataJSON: strPtr("{not json"), CreatedAt: at, UpdatedAt: at}); err != nil {
		t.Fatal(err)
	}
	f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
	f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
	f.mustReconcile()
	if !f.reconcileState("planner-1").Done {
		t.Fatal("invalid row blocked a valid candidate")
	}
}

// (1) A Worker may appear between the handoff label add and the planning label
// delete. The next tick must finish the delete instead of reporting the handoff
// as already done with stale planning labels left behind.
func TestReconcileDeliveriesHandoffCompletesLabelsAfterWorkerAppears(t *testing.T) {
	f := newDeliveryFixture(t)
	f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
	f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
	f.gh.removeErr = []error{errors.New("502 label service")}
	if err := f.reconcile(); err == nil {
		t.Fatal("expected planning label delete failure")
	}
	if labels := f.issueLabels(); !slices.Contains(labels, "looper:worker-ready") || !slices.Contains(labels, "looper:plan") {
		t.Fatalf("partial handoff labels = %v", labels)
	}
	if state := f.reconcileState("planner-1"); state.Done {
		t.Fatalf("partial handoff must stay eligible: %+v", state)
	}
	// Worker discovery reacts to the trigger and creates its loop before retry.
	f.insertLoop(storage.LoopRecord{ID: "worker-1", Type: "worker", TargetType: "issue", TargetID: strPtr(fmt.Sprintf("issue:%s:%d", deliveryTestRepo, deliveryTestIssue)), Status: "queued"}, map[string]any{"worker": map[string]any{"issueNumber": deliveryTestIssue, "repo": deliveryTestRepo}})
	f.mustReconcile()
	labels := f.issueLabels()
	if slices.Contains(labels, "looper:plan") || slices.Contains(labels, "dispatch/plan") {
		t.Fatalf("stale planning labels survived a Worker appearing: %v", labels)
	}
	state := f.reconcileState("planner-1")
	if !state.Done || state.Reason != "worker-active" {
		t.Fatalf("planner state = %+v", state)
	}
	if len(f.gh.addCalls) != 1 {
		t.Fatalf("handoff labels were added more than once: %v", f.gh.addCalls)
	}
}

// (1) A Worker loop that never delivered must not be mistaken for a completed
// handoff, but an active or delivered Worker must not be handed off twice.
func TestReconcileDeliveriesWorkerOwnershipClassification(t *testing.T) {
	t.Run("active worker is not handed off again", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
		f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
		f.insertLoop(storage.LoopRecord{ID: "worker-1", Type: "worker", TargetType: "issue", TargetID: strPtr(fmt.Sprintf("issue:%s:%d", deliveryTestRepo, deliveryTestIssue)), Status: "running"}, map[string]any{"worker": map[string]any{"issueNumber": deliveryTestIssue, "repo": deliveryTestRepo}})
		f.mustReconcile()
		if len(f.gh.addCalls) != 0 {
			t.Fatalf("active worker handoff duplicated: %v", f.gh.addCalls)
		}
		if state := f.reconcileState("planner-1"); !state.Done || state.Reason != "worker-active" {
			t.Fatalf("planner state = %+v", state)
		}
	})
	t.Run("delivered worker is not handed off again", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
		f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
		f.openImpl()
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mustReconcile()
		if len(f.gh.addCalls) != 0 {
			t.Fatalf("delivered worker handoff duplicated: %v", f.gh.addCalls)
		}
		if state := f.reconcileState("planner-1"); !state.Done || state.Reason != "worker-delivered" {
			t.Fatalf("planner state = %+v", state)
		}
	})
	t.Run("hold still blocks label work", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
		f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
		f.gh.issues[deliveryTestIssue].Labels = append(f.gh.issues[deliveryTestIssue].Labels, "looper:hold")
		f.insertLoop(storage.LoopRecord{ID: "worker-1", Type: "worker", TargetType: "issue", TargetID: strPtr(fmt.Sprintf("issue:%s:%d", deliveryTestRepo, deliveryTestIssue)), Status: "running"}, map[string]any{})
		f.mustReconcile()
		if len(f.gh.addCalls) != 0 || len(f.gh.removeCalls) != 0 {
			t.Fatalf("hold did not stop label work: adds=%v removes=%v", f.gh.addCalls, f.gh.removeCalls)
		}
		if f.reconcileState("planner-1").Done {
			t.Fatal("held handoff must stay eligible")
		}
	})
}

// (2) A partial delivery is still a real merge fact: record it, never close.
func TestReconcileDeliveriesPartialMergedRecordsFactWithoutClosing(t *testing.T) {
	f := newDeliveryFixture(t)
	f.gh.issues[deliveryTestIssue].Labels = []string{"dispatch/implement", "looper:worker-ready"}
	f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", false))
	f.mergeImpl(deliveryHeadB, deliveryMerge)
	f.mustReconcile()
	merged := f.mergedState("worker-1")
	if merged == nil || merged.MergeSHA != deliveryMerge || merged.Outcome != "partial-delivery" {
		t.Fatalf("partial merge fact = %+v", merged)
	}
	if len(f.gh.closeCalls) != 0 || len(f.gh.removeCalls) != 0 {
		t.Fatalf("partial delivery had an effect: closes=%v removes=%v", f.gh.closeCalls, f.gh.removeCalls)
	}
	if state := f.reconcileState("worker-1"); state.Done || state.IssueClosed {
		t.Fatalf("partial delivery must stay eligible and open: %+v", state)
	}
	// Once the record becomes complete with the same head, the close happens.
	record := deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true)
	f.setDeliveryRecord("worker-1", record)
	f.mustReconcile()
	if len(f.gh.closeCalls) != 1 {
		t.Fatalf("completed record did not close: %v", f.gh.closeCalls)
	}
}

// (2) Insufficient evidence waits without writing anything.
func TestReconcileDeliveriesInsufficientEvidenceWaits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(delivery.Record) delivery.Record
	}{
		{name: "metadata pending", mutate: func(r delivery.Record) delivery.Record { r.MetadataPending = true; return r }},
		{name: "not published", mutate: func(r delivery.Record) delivery.Record { r.Published = false; return r }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.seedWorker("worker-1", "running", tc.mutate(deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true)))
			f.mergeImpl(deliveryHeadB, deliveryMerge)
			f.mustReconcile()
			metadata := f.loopMetadata("worker-1")
			if _, ok := metadata[deliveryReconcileMetadataKey]; ok {
				t.Fatalf("insufficient evidence wrote state: %v", metadata[deliveryReconcileMetadataKey])
			}
			if _, ok := metadata[implementationMergedKey]; ok || len(f.gh.closeCalls) != 0 {
				t.Fatal("insufficient evidence recorded a merge or closed")
			}
		})
	}
}

// (3) Branch names are case-sensitive and a live issue read without identity
// must fail closed.
func TestReconcileDeliveriesCaseSensitiveBranchesAndClosedFail(t *testing.T) {
	t.Run("implementation base case differs", func(t *testing.T) {
		f := newDeliveryFixture(t)
		record := deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true)
		record.BaseBranch = "Main"
		f.seedWorker("worker-1", "completed", record)
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		if len(f.gh.closeCalls) != 0 || f.reconcileState("worker-1").Reason != "base-mismatch" {
			t.Fatalf("case-different base closed: closes=%v state=%+v", f.gh.closeCalls, f.reconcileState("worker-1"))
		}
	})
	t.Run("spec base case differs", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
		f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
		pr := f.gh.prs[deliveryTestSpecPR]
		pr.BaseBranch = "Main"
		f.gh.prs[deliveryTestSpecPR] = pr
		f.mustReconcile()
		if len(f.gh.addCalls) != 0 || f.reconcileState("planner-1").Reason != "base-mismatch" {
			t.Fatalf("case-different spec base handed off: adds=%v state=%+v", f.gh.addCalls, f.reconcileState("planner-1"))
		}
	})
	t.Run("live issue read without state", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.gh.issues[deliveryTestIssue].State = ""
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		if len(f.gh.closeCalls) != 0 || f.reconcileState("worker-1").Done {
			t.Fatalf("unknown issue state closed: closes=%v state=%+v", f.gh.closeCalls, f.reconcileState("worker-1"))
		}
	})
	t.Run("live issue read without number", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.gh.issues[deliveryTestIssue].Number = 0
		f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
		f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
		f.mustReconcile()
		if len(f.gh.addCalls) != 0 || f.reconcileState("planner-1").Done {
			t.Fatalf("unidentified issue handed off: adds=%v state=%+v", f.gh.addCalls, f.reconcileState("planner-1"))
		}
	})
	t.Run("closed with unknown reason is not ours", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.gh.issues[deliveryTestIssue].State, f.gh.issues[deliveryTestIssue].StateReason = "closed", "duplicate"
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		state := f.reconcileState("worker-1")
		if len(f.gh.closeCalls) != 0 || state.IssueClosed || state.Reason != string(deliveryCloseClosedUnknown) {
			t.Fatalf("external close was claimed: closes=%v state=%+v", f.gh.closeCalls, state)
		}
	})
}

// (4) A finished issue that still carries stage labels is cleaned without
// reopening, without a second close, and without claiming an external close.
func TestReconcileDeliveriesClosedIssueLabelCleanup(t *testing.T) {
	t.Run("closed completed by human", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.gh.issues[deliveryTestIssue].Labels = []string{"triaged", "dispatch/implement", "looper:worker-ready", "dispatch/plan"}
		f.gh.issues[deliveryTestIssue].State, f.gh.issues[deliveryTestIssue].StateReason = "closed", "completed"
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		state := f.reconcileState("worker-1")
		if len(f.gh.closeCalls) != 0 || state.IssueClosed || !state.LabelsReconciled || !state.Done {
			t.Fatalf("state = %+v closes=%v", state, f.gh.closeCalls)
		}
		if labels := f.issueLabels(); len(labels) != 1 || labels[0] != "triaged" {
			t.Fatalf("stage labels = %v", labels)
		}
		if f.gh.issues[deliveryTestIssue].State != "closed" {
			t.Fatal("issue was reopened")
		}
	})
	t.Run("closed not planned cleans labels only", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.gh.issues[deliveryTestIssue].Labels = []string{"dispatch/implement", "looper:worker-ready"}
		f.gh.issues[deliveryTestIssue].State, f.gh.issues[deliveryTestIssue].StateReason = "closed", "not_planned"
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		state := f.reconcileState("worker-1")
		if len(f.gh.closeCalls) != 0 || state.IssueClosed || !state.LabelsReconciled || f.gh.issues[deliveryTestIssue].StateReason != "not_planned" {
			t.Fatalf("not_planned handling = %+v closes=%v reason=%s", state, f.gh.closeCalls, f.gh.issues[deliveryTestIssue].StateReason)
		}
	})
	t.Run("closed by us keeps ownership", func(t *testing.T) {
		f := newDeliveryFixture(t)
		loop := f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.writeIntent(loop.ID, f.now.Add(-time.Minute))
		f.gh.issues[deliveryTestIssue].Labels = []string{"dispatch/implement"}
		f.gh.issues[deliveryTestIssue].State, f.gh.issues[deliveryTestIssue].StateReason = "closed", "completed"
		f.mustReconcile()
		state := f.reconcileState("worker-1")
		if len(f.gh.closeCalls) != 0 || !state.IssueClosed || !state.Done || !state.LabelsReconciled {
			t.Fatalf("own close state = %+v closes=%v", state, f.gh.closeCalls)
		}
	})
	t.Run("misassociated issue is judged before labels", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.gh.issues[deliveryTestIssue].Labels = []string{"dispatch/implement", "looper:worker-ready"}
		f.gh.issues[deliveryTestIssue].State = "closed"
		f.gh.issues[deliveryTestIssue].StateReason = "completed"
		// A pull request is not an issue: the association fails before labels.
		f.gh.issues[77] = &githubinfra.IssueDetail{Number: 77, State: "closed", StateReason: "completed", IsPullRequest: true}
		record := deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true)
		record.IssueNumber = 77
		f.seedWorker("worker-1", "completed", record)
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		if len(f.gh.removeCalls) != 0 || len(f.gh.closeCalls) != 0 {
			t.Fatalf("misassociated issue was touched: removes=%v closes=%v", f.gh.removeCalls, f.gh.closeCalls)
		}
		if state := f.reconcileState("worker-1"); !state.Done || state.Reason != string(deliveryCloseMismatch) || state.IssueClosed {
			t.Fatalf("state = %+v", state)
		}
	})
}

// (5) Holds on the implementation PR, and reviewer/fixer scope still held on
// the issue's spec PR, must keep the issue open.
func TestReconcileDeliveriesCloseRespectsPullRequestHolds(t *testing.T) {
	t.Run("implementation PR hold label", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		pr := f.gh.prs[deliveryTestImplPR]
		pr.Labels = []string{"looper:hold"}
		f.gh.prs[deliveryTestImplPR] = pr
		f.mustReconcile()
		if len(f.gh.closeCalls) != 0 || f.mergedState("worker-1") == nil || f.reconcileState("worker-1").Done {
			t.Fatalf("PR hold: closes=%v merged=%+v state=%+v", f.gh.closeCalls, f.mergedState("worker-1"), f.reconcileState("worker-1"))
		}
	})
	t.Run("spec PR reviewer scope hold", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
		f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
		f.insertLoop(storage.LoopRecord{ID: "reviewer-1", Type: "reviewer", TargetType: "pull_request", TargetID: strPtr("pr"), PRNumber: int64Ptr(deliveryTestSpecPR), Status: "paused"}, map[string]any{"reviewScopeHuman": map[string]any{"heldBy": "reviewer", "pauseReason": "review_scope_human_required"}})
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		if len(f.gh.closeCalls) != 0 {
			t.Fatalf("planning scope hold did not block the close: %v", f.gh.closeCalls)
		}
		if state := f.reconcileState("worker-1"); state.Done || state.Reason != "held-or-in-flight" {
			t.Fatalf("state = %+v", state)
		}
	})
	t.Run("spec PR hold label", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedPlanner("planner-1", deliveryTestIssue, deliveryTestSpecPR)
		f.mergeSpec(deliveryTestSpecPR, deliveryMerge)
		pr := f.gh.prs[deliveryTestSpecPR]
		pr.Labels = []string{"looper:hold:fixer"}
		f.gh.prs[deliveryTestSpecPR] = pr
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		if len(f.gh.closeCalls) != 0 {
			t.Fatalf("spec PR hold did not block the close: %v", f.gh.closeCalls)
		}
	})
	t.Run("hold released later closes", func(t *testing.T) {
		f := newDeliveryFixture(t)
		f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		pr := f.gh.prs[deliveryTestImplPR]
		pr.Labels = []string{"looper:hold:reviewer"}
		f.gh.prs[deliveryTestImplPR] = pr
		f.mustReconcile()
		if len(f.gh.closeCalls) != 0 {
			t.Fatalf("held PR closed: %v", f.gh.closeCalls)
		}
		delete(f.gh.prs, deliveryTestImplPR)
		f.mergeImpl(deliveryHeadB, deliveryMerge)
		f.mustReconcile()
		if len(f.gh.closeCalls) != 1 {
			t.Fatalf("released hold did not close: %v", f.gh.closeCalls)
		}
	})
}

func (f *deliveryFixture) setDeliveryRecord(loopID string, record delivery.Record) {
	f.t.Helper()
	current, err := f.repos.Loops.GetByID(f.ctx, loopID)
	if err != nil || current == nil {
		f.t.Fatalf("GetByID(%s) = %v", loopID, err)
	}
	metadata := deliveryMetadataObject(current.MetadataJSON)
	metadata[implementationDeliveryKey] = record
	encoded, _ := json.Marshal(metadata)
	raw := string(encoded)
	if ok, err := f.repos.Loops.UpdateMetadataIfMatch(f.ctx, loopID, &raw, f.now.Format(jsISOStringLayout), current.MetadataJSON); err != nil || !ok {
		f.t.Fatalf("set delivery record: %v %v", ok, err)
	}
}

// (2) The merge fact survives a failure to read the issue for the close decision.
func TestReconcileDeliveriesMergeFactSurvivesIssueReadFailure(t *testing.T) {
	f := newDeliveryFixture(t)
	loop := f.seedWorker("worker-1", "completed", deliveryRecord("worker-1", "worker-1-B", "worker-1-A", true))
	f.mergeImpl(deliveryHeadB, deliveryMerge)
	delete(f.gh.issues, deliveryTestIssue)
	err := f.reconcile()
	if err == nil || !strings.Contains(err.Error(), "read issue") {
		t.Fatalf("expected issue read error, got %v", err)
	}
	merged := f.mergedState(loop.ID)
	if merged == nil || merged.MergeSHA != deliveryMerge || merged.Outcome != "issue-read-failed" {
		t.Fatalf("merge fact lost: %+v", merged)
	}
	if f.reconcileState(loop.ID).Done || len(f.gh.closeCalls) != 0 {
		t.Fatal("failed issue read must not finish or close")
	}
}
