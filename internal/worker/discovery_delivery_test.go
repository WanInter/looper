package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/nexu-io/looper/internal/delivery"
	"github.com/nexu-io/looper/internal/domain"
	"github.com/nexu-io/looper/internal/loops"
	"github.com/nexu-io/looper/internal/storage"
)

const discoveryTestRepo = "acme/looper"

// discoveryRecordingLogger captures the diagnostics discovery emits for issues
// it refuses to act on, so tests can assert the operator-visible reason.
type discoveryRecordingLogger struct {
	entries []discoveryLogEntry
}

type discoveryLogEntry struct {
	level   string
	message string
	context map[string]any
}

func (l *discoveryRecordingLogger) Debug(string, map[string]any) {}
func (l *discoveryRecordingLogger) Info(message string, context map[string]any) {
	l.entries = append(l.entries, discoveryLogEntry{level: "info", message: message, context: context})
}
func (l *discoveryRecordingLogger) Warn(message string, context map[string]any) {
	l.entries = append(l.entries, discoveryLogEntry{level: "warn", message: message, context: context})
}
func (l *discoveryRecordingLogger) Error(message string, context map[string]any) {
	l.entries = append(l.entries, discoveryLogEntry{level: "error", message: message, context: context})
}

func (l *discoveryRecordingLogger) hasInfo(message, loopID, reason string) bool {
	for _, entry := range l.entries {
		if entry.level != "info" || entry.message != message || entry.context["loopId"] != loopID {
			continue
		}
		if text, _ := entry.context["reason"].(string); strings.Contains(text, reason) {
			return true
		}
	}
	return false
}

type discoveryTestForge struct {
	GitHubGateway
	state, head string
	labels      []string
	issues      []IssueSummary
	views       atomic.Int32
	onView      func()
}

func (g *discoveryTestForge) ViewPullRequest(_ context.Context, input ViewPullRequestInput) (PullRequestDetail, error) {
	g.views.Add(1)
	if g.onView != nil {
		g.onView()
	}
	return PullRequestDetail{Number: input.PRNumber, State: g.state, HeadSHA: g.head, Labels: g.labels}, nil
}

func (g *discoveryTestForge) GetCurrentUserLogin(context.Context, string) (string, error) {
	return "worker-bot", nil
}

func (g *discoveryTestForge) ListOpenIssues(context.Context, ListOpenIssuesInput) ([]IssueSummary, error) {
	return g.issues, nil
}

func discoveryTestInput(g *discoveryTestForge, issue IssueSummary) DiscoveryInput {
	issue.Labels = []string{issueDiscoveryLabel}
	g.issues = []IssueSummary{issue}
	return DiscoveryInput{ProjectID: "project_1", Repo: discoveryTestRepo}
}

func prepareDeliveryDiscovery(t *testing.T, state string) (*runnerFixture, *Runner, *discoveryTestForge, storage.ProjectRecord) {
	t.Helper()
	f := newRunnerFixture(t)
	g := &discoveryTestForge{state: state}
	r := New(Options{DB: f.coordinator.DB(), Repos: f.repos, GitHub: g, Logger: f.logger, Now: f.now})
	p, err := f.repos.Projects.GetByID(context.Background(), "project_1")
	if err != nil || p == nil {
		t.Fatalf("get project = %v, %v", p, err)
	}
	return f, r, g, *p
}

func putDeliveryDiscoveryLoop(t *testing.T, f *runnerFixture, loop storage.LoopRecord) {
	t.Helper()
	loop.ProjectID, loop.Repo = "project_1", stringPtr(discoveryTestRepo)
	loop.CreatedAt, loop.UpdatedAt = f.nowISO(), f.nowISO()
	seq, err := f.repos.Loops.AllocateSeq(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	loop.Seq = seq
	if err := f.repos.Loops.Upsert(context.Background(), loop); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryDeliveryConcurrentPRTargetDedupe(t *testing.T) {
	f, r, g, project := prepareDeliveryDiscovery(t, "OPEN")
	ctx := context.Background()
	issue := IssueSummary{Number: 520, Title: "A worker issue"}
	input := discoveryTestInput(g, issue)
	putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "planner_520", Type: "planner", Status: "completed", TargetType: "issue", TargetID: stringPtr(buildIssueTargetID(discoveryTestRepo, issue.Number)), PRNumber: int64Ptr(42), MetadataJSON: stringPtr(`{"specPath":"specs/520.md"}`)})
	const calls = 12
	start := make(chan struct{})
	errs := make(chan error, calls)
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		// Separate runners sharing the same database; no in-process runner lock.
		other := New(Options{DB: f.coordinator.DB(), Repos: f.repos, GitHub: r.github, Now: f.now})
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, err := other.DiscoverIssues(ctx, input)
			if err == nil && (len(result.QueueItems) != 1 || derefInt64(result.QueueItems[0].PRNumber) != 42) {
				err = fmt.Errorf("expected one shared PR queue item, got %#v", result)
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	allLoops, err := f.repos.Loops.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	for _, loop := range allLoops {
		if workerLoopTracksIssue(loop, project.ID, discoveryTestRepo, issue.Number) {
			count++
		}
	}
	queue, err := f.repos.Queue.List(ctx)
	if err != nil || count != 1 || len(queue) != 2 { // one fixture row and one new row
		t.Fatalf("found %d worker loops and %d total queue rows: %v", count, len(queue), err)
	}
}

func TestDiscoveryDeliveryUsesLivePlannerState(t *testing.T) {
	for _, test := range []struct {
		state, target string
	}{
		{"open", "pull_request"}, {"closed", "issue"}, {"MERGED", "issue"}, {"unknown", ""},
	} {
		t.Run(test.state, func(t *testing.T) {
			f, r, g, project := prepareDeliveryDiscovery(t, test.state)
			ctx := context.Background()
			issue := IssueSummary{Number: 521, Title: "Has a planner spec"}
			input := discoveryTestInput(g, issue)
			putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "planner_521", Type: "planner", Status: "completed", TargetType: "issue", TargetID: stringPtr(buildIssueTargetID(discoveryTestRepo, issue.Number)), PRNumber: int64Ptr(42), MetadataJSON: stringPtr(`{"specPath":"specs/521.md"}`)})
			if err := f.repos.PullRequestSnapshots.Upsert(ctx, storage.PullRequestSnapshotRecord{ID: "stale_open", ProjectID: project.ID, Repo: discoveryTestRepo, PRNumber: 42, PayloadJSON: stringPtr(`{"detail":{"state":"open"}}`), CapturedAt: f.nowISO(), CreatedAt: f.nowISO()}); err != nil {
				t.Fatal(err)
			}
			result, err := r.DiscoverIssues(ctx, input)
			if g.views.Load() != 1 {
				t.Fatalf("expected exactly one live PR lookup, got %d", g.views.Load())
			}
			if test.target == "" {
				if err == nil || !strings.Contains(err.Error(), "unknown remote state") || len(result.QueueItems) != 0 {
					t.Fatalf("unknown state = %#v, %v", result, err)
				}
				return
			}
			if err != nil || len(result.QueueItems) != 1 || result.QueueItems[0].TargetType != test.target {
				t.Fatalf("result = %#v, %v; want %s", result, err, test.target)
			}
			if !strings.Contains(derefString(result.QueueItems[0].PayloadJSON), `"specPath":"specs/521.md"`) {
				t.Fatalf("spec path missing: %#v", result.QueueItems[0])
			}
		})
	}
}

func TestDiscoveryDeliveryHistory(t *testing.T) {
	head := strings.Repeat("a", 40)
	for _, test := range []struct {
		name, state           string
		wantUndecided         string
		pr                    int64
		unpublished, verified bool
		wantRecovery, drift   bool
	}{
		{name: "explicitly unpublished", unpublished: true, wantRecovery: true},
		{name: "closed unmerged", pr: 42, state: "closed", wantRecovery: true},
		{name: "legacy merged without evidence", pr: 42, state: "merged", wantUndecided: "lacks completed implementation evidence"},
		{name: "verified merged", pr: 42, state: "merged", verified: true},
		{name: "legacy open without evidence", pr: 42, state: "open", wantUndecided: "lacks completed implementation evidence"},
		{name: "verified open", pr: 42, state: "open", verified: true},
		{name: "verified open head changed", pr: 42, state: "open", verified: true, drift: true, wantUndecided: "live head does not match"},
		{name: "verified merged head changed", pr: 42, state: "merged", verified: true, drift: true, wantUndecided: "live head does not match"},
		{name: "completed without evidence", wantUndecided: "no affirmative evidence"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, r, g, project := prepareDeliveryDiscovery(t, test.state)
			logs := &discoveryRecordingLogger{}
			r.logger = logs
			ctx := context.Background()
			issue := IssueSummary{Number: 522}
			input := discoveryTestInput(g, issue)
			g.head = head
			if test.drift {
				g.head = strings.Repeat("b", 40)
			}
			target := buildIssueTargetID(discoveryTestRepo, issue.Number)
			old := storage.LoopRecord{ID: "old", Type: "worker", Status: "completed", TargetType: "issue", TargetID: &target}
			if test.pr != 0 {
				old.TargetType, old.TargetID, old.PRNumber = "pull_request", stringPtr(fmt.Sprintf("pr:%s:%d", discoveryTestRepo, test.pr)), int64Ptr(test.pr)
			}
			meta := map[string]any{"worker": map[string]any{"issueNumber": issue.Number, "repo": discoveryTestRepo}}
			if test.unpublished || test.verified {
				record := delivery.Record{ProjectID: project.ID, Repo: discoveryTestRepo, WorkerLoopID: old.ID, RunID: "run_old", IssueNumber: issue.Number, PRNumber: test.pr, BaseBranch: "main", Published: test.verified, Complete: test.verified}
				if test.verified {
					record.HeadSHA, record.Commits = head, []string{head}
				}
				meta["implementationDelivery"] = record
			}
			old.MetadataJSON = stringPtr(mustMarshalJSON(meta))
			putDeliveryDiscoveryLoop(t, f, old)
			loopsBefore, err := f.repos.Loops.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			queueBefore, err := f.repos.Queue.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			result, err := r.DiscoverIssues(ctx, input)
			if test.wantUndecided != "" {
				// An undecidable history refuses new work without failing discovery.
				if err != nil || result.Skipped != 1 || len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 {
					t.Fatalf("result = %#v, %v; want a skip without error", result, err)
				}
				if !logs.hasInfo("worker discovery skipped an issue whose delivery history cannot be decided", old.ID, test.wantUndecided) {
					t.Fatalf("missing undecided-history diagnostic for %q: %#v", test.wantUndecided, logs.entries)
				}
				loopsAfter, err := f.repos.Loops.List(ctx)
				if err != nil {
					t.Fatal(err)
				}
				queueAfter, err := f.repos.Queue.List(ctx)
				if err != nil || len(loopsAfter) != len(loopsBefore) || len(queueAfter) != len(queueBefore) {
					t.Fatalf("undecided history persisted state: loops %d->%d, queue %d->%d, %v", len(loopsBefore), len(loopsAfter), len(queueBefore), len(queueAfter), err)
				}
				return
			}
			if err != nil || (len(result.QueueItems) == 1) != test.wantRecovery {
				t.Fatalf("result = %#v, %v; want recovery=%v", result, err, test.wantRecovery)
			}
			if !test.wantRecovery {
				return
			}
			item := result.QueueItems[0]
			if item.TargetType != "issue" || derefString(item.LoopID) == old.ID {
				t.Fatalf("should queue a new issue worker: %#v", item)
			}
			loop, err := f.repos.Loops.GetByID(ctx, *item.LoopID)
			if err != nil || loop == nil || !strings.Contains(derefString(loop.MetadataJSON), discoveryRecoverySource) {
				t.Fatalf("recovery marker: %#v, %v", loop, err)
			}
			loop.Status = "completed"
			secondRecord := delivery.Record{ProjectID: project.ID, Repo: discoveryTestRepo, WorkerLoopID: loop.ID, RunID: "run_recovery", IssueNumber: issue.Number, BaseBranch: "main"}
			meta = parseJSONObject(loop.MetadataJSON)
			meta["implementationDelivery"] = secondRecord
			loop.MetadataJSON = stringPtr(mustMarshalJSON(meta))
			if err := f.repos.Loops.Upsert(ctx, *loop); err != nil {
				t.Fatal(err)
			}
			item.Status = "completed"
			if err := f.repos.Queue.Upsert(ctx, item); err != nil {
				t.Fatal(err)
			}
			issue.Title = "changed fingerprint"
			result, err = r.DiscoverIssues(ctx, discoveryTestInput(g, issue))
			// The recovery budget is spent: refuse another automatic attempt, but
			// report it as an undecided history rather than a scheduler fault.
			if err != nil || result.Skipped != 1 || len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 {
				t.Fatalf("second recovery = %#v, %v; want a skip without error", result, err)
			}
			if !logs.hasInfo("worker discovery skipped an issue whose delivery history cannot be decided", loop.ID, "already attempted") {
				t.Fatalf("missing exhausted-recovery diagnostic: %#v", logs.entries)
			}
		})
	}
}

// A Worker run that published without passing validation commands records a
// delivery that is published but not complete. Production projects that run no
// validation commands produce only this shape, so it must not fail the tick:
// every later discovery would otherwise fail on the same unchanged history, and
// only the undecided issue — not the rest of the project — may be skipped.
func TestDiscoveryDeliveryPublishedWithoutValidationSkipsOnlyThatIssue(t *testing.T) {
	for _, state := range []string{"open", "merged"} {
		t.Run(state, func(t *testing.T) {
			f, r, g, project := prepareDeliveryDiscovery(t, state)
			logs := &discoveryRecordingLogger{}
			r.logger = logs
			ctx := context.Background()
			head := strings.Repeat("c", 40)
			g.head = head
			published := IssueSummary{Number: 530, Labels: []string{issueDiscoveryLabel}}
			fresh := IssueSummary{Number: 531, Labels: []string{issueDiscoveryLabel}}
			g.issues = []IssueSummary{published, fresh}
			record := delivery.Record{ProjectID: project.ID, Repo: discoveryTestRepo, WorkerLoopID: "published", RunID: "run_published", IssueNumber: published.Number, PRNumber: 42, BaseBranch: "main", HeadSHA: head, Commits: []string{head}, Published: true}
			meta := map[string]any{"worker": map[string]any{"issueNumber": published.Number, "repo": discoveryTestRepo}, "implementationDelivery": record}
			putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "published", Type: "worker", Status: "completed", TargetType: "pull_request", TargetID: stringPtr(fmt.Sprintf("pr:%s:42", discoveryTestRepo)), PRNumber: int64Ptr(42), MetadataJSON: stringPtr(mustMarshalJSON(meta))})

			for pass := 1; pass <= 2; pass++ {
				result, err := r.DiscoverIssues(ctx, DiscoveryInput{ProjectID: project.ID, Repo: discoveryTestRepo})
				if err != nil {
					t.Fatalf("pass %d: discovery failed on an undecided delivery: %v", pass, err)
				}
				var queued []int64
				for _, item := range result.QueueItems {
					if !strings.HasSuffix(item.TargetID, fmt.Sprintf(":%d", fresh.Number)) {
						t.Fatalf("pass %d: queued work for the undecided issue: %#v", pass, item)
					}
					queued = append(queued, fresh.Number)
				}
				if len(queued) != 1 || result.Skipped != 1 {
					t.Fatalf("pass %d: want the other issue discovered and only the undecided one skipped: %#v", pass, result)
				}
			}
			if !logs.hasInfo("worker discovery skipped an issue whose delivery history cannot be decided", "published", "lacks completed implementation evidence") {
				t.Fatalf("missing undecided-history diagnostic: %#v", logs.entries)
			}
			loop, err := f.repos.Loops.GetByID(ctx, "published")
			if err != nil || loop == nil || loop.Status != "completed" || derefString(loop.MetadataJSON) != mustMarshalJSON(meta) {
				t.Fatalf("undecided history was modified: %#v, %v", loop, err)
			}
		})
	}
}

func TestDiscoveryDeliveryReusesUnqueuedWorkerLoop(t *testing.T) {
	f, r, g, _ := prepareDeliveryDiscovery(t, "open")
	ctx := context.Background()
	issue := IssueSummary{Number: 523}
	putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "worker_orphan", Type: "worker", Status: "queued", TargetType: "pull_request", TargetID: stringPtr("pr:acme/looper:42"), PRNumber: int64Ptr(42), MetadataJSON: stringPtr(`{"worker":{"issueNumber":523,"specPath":"specs/523.md"}}`)})
	result, err := r.DiscoverIssues(ctx, discoveryTestInput(g, issue))
	if err != nil || len(result.CreatedLoopIDs) != 0 || len(result.QueueItems) != 1 || derefString(result.QueueItems[0].LoopID) != "worker_orphan" || g.views.Load() != 1 {
		t.Fatalf("result = %#v, %v; expected pending orphan PR loop reused", result, err)
	}
	if derefInt64(result.QueueItems[0].PRNumber) != 42 {
		t.Fatalf("unexpected PR: %#v", result.QueueItems[0])
	}
}

func TestDiscoveryDeliveryHeldOrRunningSuppressesHistoryRecovery(t *testing.T) {
	for _, status := range []string{"running", "paused", "awaiting_human", "human_takeover"} {
		t.Run(status, func(t *testing.T) {
			f, r, g, _ := prepareDeliveryDiscovery(t, "closed")
			ctx := context.Background()
			issue := IssueSummary{Number: 524}
			meta := stringPtr(`{"worker":{"issueNumber":524}}`)
			putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "held", Type: "worker", Status: status, TargetType: "issue", TargetID: stringPtr(buildIssueTargetID(discoveryTestRepo, issue.Number)), MetadataJSON: meta})
			putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "historical", Type: "worker", Status: "completed", TargetType: "pull_request", TargetID: stringPtr("pr:acme/looper:42"), PRNumber: int64Ptr(42), MetadataJSON: meta})
			result, err := r.DiscoverIssues(ctx, discoveryTestInput(g, issue))
			if err != nil || result.Skipped != 1 || len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 || g.views.Load() != 0 {
				t.Fatalf("result = %#v, %v; expected held/running loop to protect issue", result, err)
			}
		})
	}
}

func TestDiscoveryDeliveryPRScopeHolds(t *testing.T) {
	for _, historical := range []bool{false, true} {
		for _, kind := range []string{"scope", "budget", "pending", "fresh-label", "unrelated-repo"} {
			t.Run(fmt.Sprintf("history=%t/%s", historical, kind), func(t *testing.T) {
				f, r, g, _ := prepareDeliveryDiscovery(t, "closed")
				ctx := context.Background()
				issue := IssueSummary{Number: 525}
				if historical {
					putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "historical", Type: "worker", Status: "completed", TargetType: "pull_request", TargetID: stringPtr("pr:acme/looper:42"), PRNumber: int64Ptr(42), MetadataJSON: stringPtr(`{"worker":{"issueNumber":525}}`)})
				} else {
					putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "planner", Type: "planner", Status: "completed", TargetType: "issue", TargetID: stringPtr(buildIssueTargetID(discoveryTestRepo, issue.Number)), PRNumber: int64Ptr(42)})
				}
				var metadata string
				var err error
				role := "reviewer"
				switch kind {
				case "scope", "unrelated-repo":
					metadata, err = loops.WriteReviewScopeHumanState(nil, loops.ReviewScopeHumanState{HeldBy: "reviewer"})
				case "budget":
					role = "fixer"
					metadata, err = loops.WriteReviewFixBudgetState(nil, loops.ReviewFixBudgetState{PauseReason: loops.ReviewFixBudgetTerminationReason})
				case "pending":
					metadata, err = loops.PersistPendingReviewScopeHumanEvidence(nil, "Needs a human", "evidence", true)
				case "fresh-label":
					g.labels = []string{domain.HoldLabelReviewer}
				}
				if err != nil {
					t.Fatal(err)
				}
				if metadata != "" {
					target := "pr:ACME/LOOPER:42" // Legacy target-only row; no pr_number.
					if kind == "unrelated-repo" {
						target = "pr:acme/unrelated:42"
					}
					putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "hold", Type: role, Status: "paused", TargetType: "pull_request", TargetID: &target, MetadataJSON: &metadata})
				}
				result, err := r.DiscoverIssues(ctx, discoveryTestInput(g, issue))
				if kind == "unrelated-repo" {
					if err != nil || len(result.QueueItems) != 1 {
						t.Fatalf("unrelated hold suppressed discovery: %#v, %v", result, err)
					}
					return
				}
				if err != nil || result.Skipped != 1 || len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 {
					t.Fatalf("scope hold did not block new work: %#v, %v", result, err)
				}
			})
		}
	}
}

func TestDiscoveryDeliveryRechecksProjectArchivedAfterLiveLookup(t *testing.T) {
	f, r, g, project := prepareDeliveryDiscovery(t, "open")
	ctx := context.Background()
	issue := IssueSummary{Number: 526}
	putDeliveryDiscoveryLoop(t, f, storage.LoopRecord{ID: "planner", Type: "planner", Status: "completed", TargetType: "issue", TargetID: stringPtr(buildIssueTargetID(discoveryTestRepo, issue.Number)), PRNumber: int64Ptr(42)})
	g.onView = func() {
		project.Archived = true
		if err := f.repos.Projects.Upsert(ctx, project); err != nil {
			t.Fatal(err)
		}
	}
	result, err := r.DiscoverIssues(ctx, discoveryTestInput(g, issue))
	if err != nil || result.Skipped != 1 || len(result.QueueItems) != 0 || len(result.CreatedLoopIDs) != 0 || g.views.Load() != 1 {
		t.Fatalf("archived project discovery = %#v, %v", result, err)
	}
}
