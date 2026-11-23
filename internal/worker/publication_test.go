package worker

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nexu-io/looper/internal/config"
	"github.com/nexu-io/looper/internal/delivery"
	"github.com/nexu-io/looper/internal/infra/specpr"
	"github.com/nexu-io/looper/internal/lifecycle"
	"github.com/nexu-io/looper/internal/storage"
)

type publicationForge struct {
	*fakeGitHubGateway
	afterWrite bool
	failRead   bool
	body       string
}

func (f *publicationForge) ViewPullRequest(ctx context.Context, i ViewPullRequestInput) (PullRequestDetail, error) {
	if f.afterWrite && f.failRead {
		f.failRead = false
		return PullRequestDetail{}, errors.New("read-back unavailable")
	}
	return PullRequestDetail{Number: i.PRNumber, URL: "https://example/acme/looper/pull/101", HeadSHA: strings.Repeat("a", 40), BaseRefName: "main", Body: f.body}, nil
}
func (f *publicationForge) UpdatePullRequestBody(ctx context.Context, i UpdatePullRequestBodyInput) error {
	f.body = i.Body
	f.afterWrite = true
	f.updatePRBodyCalls = append(f.updatePRBodyCalls, i)
	return nil
}

func TestPublicationResumesAfterReadbackFailure(t *testing.T) {
	ctx := context.Background()
	fixture := newRunnerFixture(t)
	sha := strings.Repeat("a", 40)
	git := &fakeGitGateway{createResult: CreateWorktreeResult{WorktreePath: t.TempDir(), Branch: "looper/feature", BaseBranch: "main", HeadSHA: strings.Repeat("b", 40), WorktreeID: "wt"}, inspectResult: InspectHeadResult{HeadSHA: sha, NewCommitSHAs: []string{sha}, CommittedFiles: []string{"feature.go"}}}
	git.inspectResults = []InspectHeadResult{{HasUncommittedChanges: true, ChangedFiles: []string{"feature.go"}}}
	git.commitResult = CommitResult{CommitSHA: sha}
	forge := &publicationForge{fakeGitHubGateway: &fakeGitHubGateway{createPRResult: CreatePullRequestResult{Number: 101, URL: "https://example/acme/looper/pull/101"}}, body: "Human introduction", failRead: true}
	agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "implemented", Stdout: `__LOOPER_RESULT__={"summary":"implemented","issueNumber":27,"issueResolution":"full"}`, ParseStatus: "parsed"}}}
	agent.wait = func(context.Context) error { return os.MkdirAll(git.createResult.WorktreePath, 0700) }
	opts := Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: forge, Git: git, AgentExecutor: agent, Now: fixture.now, AllowAutoCommit: true, AllowAutoPush: true, OpenPRStrategy: config.OpenPRStrategyAllDone, ValidationCommands: []string{"true"}}
	r := New(opts)
	run := func() ProcessResult {
		claim, err := fixture.repos.Queue.ClaimNextOfType(ctx, fixture.nowISO(), "worker", "worker")
		if err != nil || claim == nil {
			t.Fatalf("claim: %v %v", claim, err)
		}
		result, err := r.ProcessClaimedItem(ctx, *claim)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	record := func() *delivery.Record {
		loop, err := fixture.repos.Loops.GetByID(ctx, "loop_worker_1")
		if err != nil {
			t.Fatal(err)
		}
		d, err := delivery.FromMetadata(loop.MetadataJSON)
		if err != nil || d == nil {
			t.Fatalf("delivery %v: %v", d, err)
		}
		return d
	}
	first := run()
	if first.FailureKind != FailureRetryableAfterResume || !strings.Contains(first.Summary, "read-back unavailable") {
		t.Fatalf("first: %+v", first)
	}
	d := record()
	if !d.MetadataPending || d.Published || d.Complete {
		t.Fatalf("pending: %+v", d)
	}
	inspections, commits := len(git.inspectCalls), len(git.commitCalls)
	if commits != 1 {
		t.Fatalf("initial commits = %d, want one fallback commit", commits)
	}
	r = New(opts) // No in-memory worker state survives this restart.
	fixture.advance(5 * time.Second)
	second := run()
	if second.Status != "success" {
		t.Fatalf("second: %+v", second)
	}
	d = record()
	if !d.Complete || !d.VerifiedFor("project_1", "acme/looper", 27, 101) || d.RunID != first.RunID || d.MetadataPending {
		t.Fatalf("confirmed: %+v", d)
	}
	if len(agent.starts) != 1 || len(git.pushCalls) != 1 || len(git.inspectCalls) != inspections || len(git.commitCalls) != commits || len(forge.updatePRBodyCalls) != 1 {
		t.Fatal("publication retry repeated completed side effects")
	}
}

func TestPublicationEvidenceAndManualBody(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, name := range []string{"partial", "docs-only", "validation-head-drift", "no-commands", "human-closing", "remote-head-drift", "wrong-repo", "wrong-url-number", "wrong-pr", "wrong-base"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := newRunnerFixture(t)
			run := storage.RunRecord{ID: "run_publish", LoopID: "loop_worker_1", Status: "running", StartedAt: f.nowISO(), CreatedAt: f.nowISO(), UpdatedAt: f.nowISO()}
			if err := f.repos.Runs.Upsert(ctx, run); err != nil {
				t.Fatal(err)
			}
			p := PullRequestDetail{Number: 101, URL: "https://example/acme/looper/pull/101", BaseRefName: "main", HeadSHA: sha, Body: "Human notes"}
			w := workerInput{ExecutionMode: "push-existing", PRNumber: p.Number, Repo: "acme/looper", IssueNumber: 27, BaseBranch: "main"}
			e := checkpointExecution{LocalHead: sha, LocalCommits: []string{sha}, CommittedFiles: []string{"foo.go"}, Status: "completed", ParseStatus: "parsed", Summary: "done", Stdout: `__LOOPER_RESULT__={"summary":"done","issueNumber":27,"issueResolution":"full"}`}
			v := ValidationResult{RunID: "previous", PRFiles: []string{"foo.go"}, Passed: true, ValidatedHead: sha, Commands: []string{"go test"}}
			reject := false
			switch name {
			case "partial":
				e.Stdout = strings.Replace(e.Stdout, "full", "partial", 1)
			case "docs-only":
				v.PRFiles = []string{"docs/spec.md"} // This run removed foo.go's earlier changes; the final PR only changes docs.
			case "validation-head-drift":
				v.ValidatedHead = fakeWorkerHead
			case "no-commands":
				v.Commands = nil
			case "human-closing":
				e.Stdout = strings.Replace(e.Stdout, "full", "partial", 1)
				p.Body += "\nCloses #27"
				reject = true
			case "remote-head-drift":
				p.HeadSHA = fakeWorkerHead
				reject = true
			case "wrong-repo":
				p.URL = "https://example/acme/other/pull/101"
				reject = true
			case "wrong-url-number":
				p.URL = "https://example/acme/looper/pull/100"
				reject = true
			case "wrong-pr":
				p.Number = 100
				reject = true
			case "wrong-base":
				p.BaseRefName = "release"
				reject = true
			}
			g := &fakeGitHubGateway{prDetail: p}
			r := New(Options{DB: f.coordinator.DB(), Repos: f.repos, GitHub: g, Now: f.now})
			in := stepInput{Project: storage.ProjectRecord{ID: "project_1"}, Loop: storage.LoopRecord{ID: run.LoopID}, Run: run}
			c, err := r.finishPublication(ctx, in, workerCheckpoint{Work: &w, Execution: &e, Validation: &v, PullRequest: &checkpointPullPR{Number: 101}})
			if c.Delivery != nil && c.Delivery.Complete {
				t.Fatalf("invalid evidence: %+v", c.Delivery)
			}
			if reject {
				if err == nil || len(g.updatePRBodyCalls) != 0 || g.prDetail.Body != p.Body || (name == "human-closing") != (c.Delivery != nil && c.Delivery.MetadataPending) {
					t.Fatalf("changed human body or accepted bad binding: %v, %+v", err, c.Delivery)
				}
				if name == "human-closing" {
					var failure *loopError
					if !errors.As(err, &failure) || failure.kind != FailureManualIntervention {
						t.Fatalf("expected manual intervention: %v", err)
					}
				}
				return
			}
			if err != nil || c.Delivery == nil || !c.Delivery.Published || !strings.Contains(g.prDetail.Body, "Refs #27") || closingKeyword.MatchString(g.prDetail.Body) {
				t.Fatalf("partial publication: %v %+v %q", err, c.Delivery, g.prDetail.Body)
			}
			if name == "partial" && c.Delivery.ValidationRef != "previous:validate" {
				t.Fatalf("wrong validation owner: %s", c.Delivery.ValidationRef)
			}
		})
	}
}

func TestPublicationOwnsOnlyMarkedPlannerSummary(t *testing.T) {
	w := workerInput{Repo: "acme/test", IssueNumber: 42, ExecutionMode: "push-existing", SpecPath: "docs/spec.md"}
	d := delivery.Record{SpecPath: w.SpecPath, HeadSHA: strings.Repeat("a", 40)}
	old := specpr.ManagedPlanStart + "\n## Summary\n- Adds the planning spec for acme/test#42\n- Spec path: docs/spec.md\n- Planner branch: topic\n\n## Agent Summary\nplanning only\n" + specpr.ManagedPlanEnd
	body := "before\n" + old + "\nSpec: docs/spec.md\nIssue: acme/test#42\nHuman notes"
	e := &checkpointExecution{Summary: "Fixes #42"}
	got, err := publicationBody(body, w, d, e, true)
	if err != nil || strings.Contains(got, "planning only") || strings.Contains(got, "Fixes #42") || !strings.HasPrefix(got, "before\n") || !strings.HasSuffix(got, "Human notes") {
		t.Fatalf("body %q, %v", got, err)
	}
	again, err := publicationBody(got, w, d, e, true)
	if err != nil || again != got {
		t.Fatal("not idempotent")
	}
	if _, err := publicationBody(body+specpr.ManagedPlanEnd, w, d, e, true); err == nil {
		t.Fatal("ambiguous planner block replaced")
	}
}

func TestPublicationRejectsConflictingDurableIssue(t *testing.T) {
	d := delivery.Record{ProjectID: "p", Repo: "acme/test", IssueNumber: 5, PRNumber: 10}
	l := storage.LoopRecord{ProjectID: "p", Type: "worker", Repo: stringPtr(d.Repo), PRNumber: int64Ptr(10), MetadataJSON: stringPtr(`{"worker":{"issueNumber":6}}`)}
	if uniquePublicationIssue([]storage.LoopRecord{l}, d) {
		t.Fatal("ignored conflicting source issue")
	}
}

func TestPublicationValidationObservesGit(t *testing.T) {
	for _, test := range []string{"dirty-before", "dirty-after", "head-drift", "clean"} {
		t.Run(test, func(t *testing.T) {
			before := InspectHeadResult{HeadSHA: fakeWorkerHead}
			after := before
			after.CommittedFiles = []string{"feature.go"}
			switch test {
			case "dirty-before":
				before.HasUncommittedChanges = true
			case "dirty-after":
				after.HasUncommittedChanges = true
			case "head-drift":
				after.HeadSHA = strings.Repeat("b", 40)
			}
			g := &fakeGitGateway{inspectResults: []InspectHeadResult{before, after}}
			ran := 0
			r := New(Options{Git: g, ValidationCommands: []string{"test"}, ValidationRunner: func(context.Context, ValidationInput) (ValidationResult, error) {
				ran++
				return ValidationResult{Passed: true}, nil
			}})
			c, err := r.runValidateStep(context.Background(), stepInput{Project: storage.ProjectRecord{ID: "p", RepoPath: t.TempDir()}, Run: storage.RunRecord{ID: "validation-A"}, Checkpoint: workerCheckpoint{Work: &workerInput{IssueNumber: 27, BaseBranch: "main"}, Worktree: &checkpointWorktree{}, Execution: &checkpointExecution{LocalHead: "different-execution-head"}}})
			if test == "dirty-before" && ran != 0 {
				t.Fatal("validation ran on dirty tree")
			}
			if test == "clean" {
				if err != nil || c.Validation.ValidatedHead != fakeWorkerHead || c.Validation.RunID != "validation-A" || len(g.inspectCalls) != 2 || g.inspectCalls[1].BaseRef != "main" {
					t.Fatalf("validation not bound: %+v %v", c.Validation, err)
				}
			} else if err == nil || c.Validation != nil && c.Validation.ValidatedHead != "" {
				t.Fatal("dirty or drifting checkout certified")
			}
		})
	}
}

func TestPublicationPreflightAndAgentFastForward(t *testing.T) {
	for _, test := range []string{"agent-fast-forward", "agent-diverged", "closing-before-push"} {
		t.Run(test, func(t *testing.T) {
			ctx := context.Background()
			f := newRunnerFixture(t)
			pr := int64(101)
			local := fakeWorkerHead
			remote := strings.Repeat("a", 40)
			p := PullRequestDetail{Number: pr, URL: "https://example/acme/looper/pull/101", BaseRefName: "main", HeadRefName: "topic", HeadSHA: remote, State: "OPEN"}
			g := &fakeGitHubGateway{prDetail: p}
			git := &fakeGitGateway{inspectResult: InspectHeadResult{HeadSHA: local, NewCommitSHAs: []string{remote, local}}}
			e := &checkpointExecution{Status: "completed", Summary: "implemented", ParseStatus: "parsed"}
			work := &workerInput{Repo: "acme/looper", IssueNumber: 27, ExecutionMode: "create-pr", BaseBranch: "main", PRNumber: pr}
			if test == "agent-fast-forward" {
				g.prDetailResponses = []PullRequestDetail{p}
				g.prDetail.HeadSHA = local
			} else if test == "agent-diverged" {
				git.inspectResult.NewCommitSHAs = []string{local}
			} else {
				work.ExecutionMode = "push-existing"
				g.prDetail.Body = "Human notes\nCloses #27"
			}
			r := New(Options{DB: f.coordinator.DB(), Repos: f.repos, Git: git, GitHub: g, AllowAutoPush: true, Now: f.now})
			project, _ := f.repos.Projects.GetByID(ctx, "project_1")
			loop, _ := f.repos.Loops.GetByID(ctx, "loop_worker_1")
			queue, _ := f.repos.Queue.GetByID(ctx, "queue_worker_1")
			run := storage.RunRecord{ID: "publish", LoopID: loop.ID, Status: "running", StartedAt: f.nowISO(), UpdatedAt: f.nowISO(), CreatedAt: f.nowISO()}
			if err := f.repos.Runs.Upsert(ctx, run); err != nil {
				t.Fatal(err)
			}
			c, err := r.runOpenPRStep(ctx, stepInput{Project: *project, Loop: *loop, QueueItem: *queue, Run: run, Checkpoint: workerCheckpoint{Work: work, Worktree: &checkpointWorktree{Branch: "topic", BaseBranch: "main"}, Execution: e, PullRequest: &checkpointPullPR{Number: pr, URL: p.URL}, Lifecycle: &lifecycle.State{Branch: "topic", BaseBranch: "main", Pushed: true, PRNumber: pr, Actions: lifecycle.Actions{Push: lifecycle.ActionSourceAgent, PR: lifecycle.ActionSourceAgent}}}})
			if test == "agent-fast-forward" {
				if err != nil || len(git.pushCalls) != 1 || c.Delivery == nil || !c.Delivery.Published || c.Delivery.HeadSHA != local {
					t.Fatalf("fast-forward failed: %v %+v", err, c.Delivery)
				}
			} else if err == nil || len(git.pushCalls) != 0 || c.Delivery != nil || len(g.updatePRBodyCalls) != 0 {
				t.Fatalf("unsafe publication: %v %+v", err, c.Delivery)
			}
		})
	}
}

func TestPublicationDamagedPendingDoesNotStartNewRun(t *testing.T) {
	ctx := context.Background()
	f := newRunnerFixture(t)
	l, _ := f.repos.Loops.GetByID(ctx, "loop_worker_1")
	d := delivery.Record{ProjectID: l.ProjectID, WorkerLoopID: l.ID, Repo: "acme/looper", IssueNumber: 27, PRNumber: 101, MetadataPending: true}
	raw := mustMarshalJSON(map[string]any{"implementationDelivery": d})
	l.MetadataJSON = &raw
	run := storage.RunRecord{ID: "interrupted", LoopID: l.ID, Status: "interrupted", CreatedAt: f.nowISO(), UpdatedAt: f.nowISO(), StartedAt: f.nowISO()}
	r := New(Options{DB: f.coordinator.DB(), Repos: f.repos, Now: f.now})
	for _, c := range []workerCheckpoint{{}, {Delivery: &d, Execution: &checkpointExecution{Status: "invalid"}}} {
		s := mustMarshalJSON(c)
		run.CheckpointJSON = &s
		if err := f.repos.Runs.Upsert(ctx, run); err != nil {
			t.Fatal(err)
		}
		if _, err := r.createRunContext(ctx, *l); err == nil {
			t.Fatal("corrupt pending replayed")
		}
		latest, _ := f.repos.Runs.GetLatestByLoopID(ctx, l.ID)
		if latest.ID != run.ID {
			t.Fatal("unexpected new run")
		}
	}
}

func TestPublicationIncompleteSignals(t *testing.T) {
	for _, stdout := range []string{"", `__LOOPER_RESULT__={"summary":"done","issueNumber":27,"issueResolution":"partial"}`, `__LOOPER_RESULT__={"summary":"done","issueNumber":28,"issueResolution":"full"}`} {
		if declaredFull(&checkpointExecution{Status: "completed", ParseStatus: "parsed", Summary: "done", Stdout: stdout}, 27) {
			t.Errorf("accepted %s", stdout)
		}
	}
	if implementationFiles([]string{"docs/example.go", "README.md", "specs/test.sh"}, "") {
		t.Fatal("docs classified as implementation")
	}
}
