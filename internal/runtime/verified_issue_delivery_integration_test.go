package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nexu-io/looper/internal/agent"
	"github.com/nexu-io/looper/internal/config"
	"github.com/nexu-io/looper/internal/coordinator"
	"github.com/nexu-io/looper/internal/delivery"
	"github.com/nexu-io/looper/internal/e2e/harness"
	gitinfra "github.com/nexu-io/looper/internal/infra/git"
	githubinfra "github.com/nexu-io/looper/internal/infra/github"
	"github.com/nexu-io/looper/internal/storage"
	"github.com/nexu-io/looper/internal/worker"
)

const (
	verifiedDeliveryRepo       = "acme/verified-delivery"
	verifiedDeliveryURL        = "https://github.com/" + verifiedDeliveryRepo
	verifiedDeliveryIssueRoute = "repos/" + verifiedDeliveryRepo + "/issues/42"
	verifiedDeliveryTitle      = "Implement greeting"
	verifiedDeliveryIssue      = 42
	verifiedDeliverySpecPR     = 101
	verifiedDeliveryImplPR     = 102
	verifiedDeliveryTime       = "2026-05-14T12:00:00.000Z"
	verifiedDeliveryReady      = "looper:worker-ready"
)

func TestVerifiedIssueDeliveryLifecycle(t *testing.T) {
	for _, strategy := range []string{"merge", "squash", "rebase"} {
		t.Run(strategy, func(t *testing.T) { verifyIssueDeliveryLifecycle(t, strategy) })
	}
}

func verifyIssueDeliveryLifecycle(t *testing.T, strategy string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	t.Setenv("LOOPER_E2E_ARTIFACT_ROOT", "")
	home := harness.NewTempHome(t)
	t.Setenv("HOME", home.HomeDir)
	t.Setenv("XDG_CONFIG_HOME", home.HomeDir)
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_ALLOW_PROTOCOL", "file")
	for _, key := range []string{"GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_CONFIG_PARAMETERS", "GIT_TEMPLATE_DIR"} {
		t.Setenv(key, "") // Preserve the ambient value for testing's cleanup.
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("GIT_CONFIG_COUNT", "0")
	gitPath, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	repo := harness.CreateSeededRepo(t, gitPath)
	origin := harness.CreateBareOrigin(t, gitPath, repo.Path)
	git := func(cwd string, args ...string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, gitPath, args...)
		cmd.Dir = cwd
		b, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, b)
		}
		return strings.TrimSpace(string(b))
	}
	const specBranch = "looper/planner/42-implement-greeting"
	const specPath = "specs/2026-05-14-42-implement-greeting.md"
	specHead := harness.CreateBranchCommitAndPush(t, gitPath, repo.Path, specBranch, specPath, "# Greeting\n\n`greeting.sh` must print `hello, delivery`.\n")
	git(repo.Path, "merge", "--no-ff", specBranch, "-m", "Manually merge planning spec")
	specMerge := git(repo.Path, "rev-parse", "HEAD")
	// A later base tip exercises the gateway's ancestry comparison, not just
	// the equality shortcut. It intentionally leaves the spec intact.
	git(repo.Path, "commit", "--allow-empty", "-m", "Advance main after the spec merge")
	git(repo.Path, "push", "origin", "main")
	if specMerge == specHead {
		t.Fatal("fixture must have a distinct spec merge commit")
	}

	statePath := filepath.Join(home.Root, "gh-state.json")
	t.Setenv("LOOPER_VERIFIED_DELIVERY_GH_STATE", statePath)
	state := verifiedDeliveryGHState{
		Origin: origin, GitPath: gitPath, IssueState: "open", IssueLabels: []string{"triaged", "dispatch/plan", "looper:plan"},
		Spec: verifiedDeliveryPR{Number: verifiedDeliverySpecPR, Title: "Plan greeting", Body: "Refs #42", Branch: specBranch, BaseSHA: repo.InitialCommit, HeadSHA: specHead, MergeSHA: specMerge, Labels: []string{"looper:spec-ready"}},
	}
	verifiedDeliveryWriteState(t, statePath, state)
	fixture := func(name string) string {
		t.Helper()
		contents, err := os.ReadFile(filepath.Join("..", "..", "testdata", "verified_delivery", name))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(home.Root, name)
		if err := os.WriteFile(path, contents, 0o700); err != nil {
			t.Fatal(err)
		}
		return path
	}
	ghPath := fixture("gh.sh")
	testBinary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LOOPER_VERIFIED_DELIVERY_TEST_BINARY", testBinary)
	// Even an incidental `gh` lookup from a child resolves to the fixture.
	if err := os.Symlink(ghPath, filepath.Join(home.Root, "gh")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", home.Root+string(os.PathListSeparator)+os.Getenv("PATH"))
	agentPath := fixture("agent.sh")
	if err := os.Symlink(agentPath, filepath.Join(home.Root, "codex")); err != nil {
		t.Fatal(err)
	}
	openDB := func() *storage.SQLiteCoordinator {
		t.Helper()
		db, err := storage.OpenSQLiteCoordinator(ctx, home.DBPath, storage.SQLiteCoordinatorOptions{Migrations: storage.EmbeddedMigrations})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		if _, err := db.MigrationRunner().RunPending(ctx); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := openDB()
	repos := storage.NewRepositories(db.DB())
	const projectID = "verified-delivery"
	const target = "issue:" + verifiedDeliveryRepo + ":42"
	base := "main"
	projectMetadata, _ := json.Marshal(map[string]string{"repo": verifiedDeliveryRepo, "worktreeRoot": home.WorktreeRoot})
	if err := repos.Projects.Upsert(ctx, storage.ProjectRecord{ID: projectID, Name: "Verified Delivery", RepoPath: repo.Path, BaseBranch: &base, MetadataJSON: stringPtr(string(projectMetadata)), CreatedAt: verifiedDeliveryTime, UpdatedAt: verifiedDeliveryTime}); err != nil {
		t.Fatal(err)
	}
	// Seed only Planner's completed association. The Worker creates all its own evidence.
	specPR := int64(verifiedDeliverySpecPR)
	plannerMetadata, _ := json.Marshal(map[string]any{"issueNumber": verifiedDeliveryIssue, "issueTitle": verifiedDeliveryTitle, "specPath": specPath, "branch": specBranch, "prNumber": specPR, "prUrl": verifiedDeliveryURL + "/pull/101"})
	const plannerLoopID = "planner-delivery"
	if err := repos.Loops.Upsert(ctx, storage.LoopRecord{ID: plannerLoopID, ProjectID: projectID, Type: "planner", TargetType: "issue", TargetID: stringPtr(target), Repo: stringPtr(verifiedDeliveryRepo), PRNumber: &specPR, Status: "completed", MetadataJSON: stringPtr(string(plannerMetadata)), LastRunAt: stringPtr(verifiedDeliveryTime), CreatedAt: verifiedDeliveryTime, UpdatedAt: verifiedDeliveryTime}); err != nil {
		t.Fatal(err)
	}
	plannerCheckpoint, _ := json.Marshal(map[string]any{
		"resumePolicy": "advance_from_checkpoint",
		"issue":        map[string]any{"repo": verifiedDeliveryRepo, "issueNumber": verifiedDeliveryIssue, "title": verifiedDeliveryTitle, "url": verifiedDeliveryURL + "/issues/42", "specPath": specPath},
		"worktree":     map[string]string{"branch": specBranch, "baseBranch": base, "specPath": specPath},
		"writeSpec":    map[string]any{"status": "completed", "commits": []string{specHead}, "gitReconciled": true},
		"publish":      map[string]any{"pushed": true, "pullRequest": map[string]any{"number": specPR, "url": verifiedDeliveryURL + "/pull/101", "body": state.Spec.Body}, "labelsAdded": []string{"looper:spec-reviewing"}},
		"notify":       map[string]string{"sentAt": verifiedDeliveryTime, "message": "Spec PR ready for review: " + verifiedDeliveryURL + "/pull/101"},
	})
	if err := repos.Runs.Upsert(ctx, storage.RunRecord{ID: "planner-delivery-run", LoopID: plannerLoopID, Status: "completed", LastCompletedStep: stringPtr("notify"), CheckpointJSON: stringPtr(string(plannerCheckpoint)), StartedAt: verifiedDeliveryTime, LastHeartbeatAt: stringPtr(verifiedDeliveryTime), EndedAt: stringPtr(verifiedDeliveryTime), CreatedAt: verifiedDeliveryTime, UpdatedAt: verifiedDeliveryTime}); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.DefaultConfig(repo.Path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Roles.Coordinator.Enabled = true
	cfg.Roles.Reviewer.Behavior.ReviewEvents.AutoImplement = true
	cfg.Roles.Worker.AutoDiscovery = true
	cfg.Roles.Worker.Triggers.Labels = []string{verifiedDeliveryReady}
	cfg.Roles.Worker.Triggers.RequireAssigneeCurrentUser = false
	cfg.Projects = []config.ProjectRefConfig{{ID: projectID, Name: "Verified Delivery", Repo: verifiedDeliveryRepo, RepoPath: repo.Path, BaseBranch: &base, WorktreeRoot: &home.WorktreeRoot}}
	now := func() time.Time {
		value, _ := time.Parse(time.RFC3339Nano, verifiedDeliveryTime)
		return value
	}
	newGH := func() *githubinfra.Gateway {
		return githubinfra.New(githubinfra.Options{GHPath: ghPath, CWD: repo.Path, Now: now})
	}
	newCoordinator := func(repos *storage.Repositories) *coordinator.Runner {
		return coordinator.New(coordinator.Options{Repos: repos, GitHub: newGH(), Config: &cfg, Now: now})
	}
	gh := newGH()
	vendor := config.AgentVendorCodex
	newWorker := func() *worker.Runner {
		return worker.New(worker.Options{
			DB: db.DB(), Repos: repos, GitHub: workerGitHubAdapter{gateway: gh, config: &cfg},
			Git: workerGitAdapter{gateway: gitinfra.New(gitinfra.Options{GitPath: gitPath, Repos: repos, Now: now})},
			AgentExecutor: workerAgentExecutorAdapter{executor: agent.New(agent.ExecutorOptions{
				Config:            agent.ExecutorConfig{Vendor: vendor, Params: map[string]any{"command": agentPath}},
				ParamsOwnerVendor: &vendor, Repos: repos, LogDir: home.LogDir, Now: now,
			})},
			Now: now, AgentRuntime: string(vendor), CustomInstructions: &cfg,
			AllowAutoCommit: true, AllowAutoPush: true, OpenPRStrategy: config.OpenPRStrategyAllDone,
			ValidationCommands: []string{`test "$(sh greeting.sh)" = 'hello, delivery'`},
		})
	}
	coordInput := coordinator.DiscoveryInput{ProjectID: projectID, Repo: verifiedDeliveryRepo}
	workerInput := worker.DiscoveryInput{ProjectID: projectID, Repo: verifiedDeliveryRepo}
	coord, implementer := newCoordinator(repos), newWorker()
	before, err := implementer.DiscoverIssues(ctx, workerInput)
	if err != nil || len(before.QueueItems) != 0 || len(before.CreatedLoopIDs) != 0 {
		t.Fatalf("Worker discovery before plan handoff: %+v, %v", before, err)
	}
	handoffs := make(chan error, 2)
	for range 2 {
		go func() { handoffs <- coord.ReconcileDeliveries(ctx, coordInput) }()
	}
	for range 2 {
		if err := <-handoffs; err != nil {
			t.Fatalf("reconcile merged spec: %v", err)
		}
	}
	state = verifiedDeliveryReadState(t, statePath)
	if state.IssueState != "open" || !slices.Contains(state.IssueLabels, verifiedDeliveryReady) || state.Impl != nil {
		t.Fatalf("merged spec did not leave the issue open and ready for Worker: %+v", state)
	}
	t.Log("merged spec handed off; issue remains open")

	// Two separate normal discoverers race on the same issue and persistent DB.
	start := make(chan struct{})
	done := make(chan error, 2)
	for _, runner := range []*worker.Runner{implementer, newWorker()} {
		go func() {
			<-start
			_, err := runner.DiscoverIssues(ctx, workerInput)
			done <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-done; err != nil {
			t.Errorf("concurrent Worker discovery: %v", err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	if _, err := implementer.DiscoverIssues(ctx, workerInput); err != nil {
		t.Fatalf("repeated Worker discovery: %v", err)
	}
	loops, err := repos.Loops.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	items, err := repos.Queue.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loops) != 2 || len(items) != 1 || items[0].Type != "worker" || items[0].TargetID != target || items[0].Status != "queued" {
		t.Fatalf("want one Planner loop, one Worker loop and one queued Worker; loops=%+v items=%+v", loops, items)
	}
	t.Log("concurrent and repeated discovery left exactly one Worker loop and queue item")
	result, err := implementer.ProcessNext(ctx, "delivery-integration")
	if err != nil || result == nil || result.Status != "success" || result.PullRequestNumber != verifiedDeliveryImplPR {
		t.Fatalf("Worker publish: %+v, %v", result, err)
	}
	workerLoop, err := repos.Loops.GetByID(ctx, result.LoopID)
	if err != nil || workerLoop == nil {
		t.Fatalf("read published Worker loop: %+v, %v", workerLoop, err)
	}
	record, err := delivery.FromMetadata(workerLoop.MetadataJSON)
	if err != nil || record == nil || !record.VerifiedFor(projectID, verifiedDeliveryRepo, verifiedDeliveryIssue, verifiedDeliveryImplPR) || !record.Complete || record.MetadataPending || record.RunID != result.RunID || record.WorkerLoopID != result.LoopID || record.SpecPath != specPath || record.ValidationRef == "" {
		t.Fatalf("Worker did not persist matching complete delivery: %+v, %v; metadata=%s", record, err, derefString(workerLoop.MetadataJSON))
	}
	state = verifiedDeliveryReadState(t, statePath)
	if state.Impl == nil || state.Impl.Number != verifiedDeliveryImplPR || state.Impl.HeadSHA != record.HeadSHA || state.Impl.MergeSHA != "" || state.PRCreates != 1 {
		t.Fatalf("expected exactly one open implementation PR matching Worker evidence: %+v", state)
	}
	if !strings.Contains(state.Impl.Body, "Refs #42") || strings.Contains(state.Impl.Body, "Closes #42") {
		t.Fatalf("Worker PR should reference the issue without closing keywords: %q", state.Impl.Body)
	}
	if got := git(origin, "show", "refs/heads/"+state.Impl.Branch+":greeting.sh"); got != "printf '%s\\n' 'hello, delivery'" {
		t.Fatalf("published implementation content = %q", got)
	}
	t.Log("Worker validated, pushed, and recorded the implementation PR")
	if err := coord.ReconcileDeliveries(ctx, coordInput); err != nil {
		t.Fatalf("reconcile open implementation: %v", err)
	}
	state = verifiedDeliveryReadState(t, statePath)
	if state.IssueState != "open" || state.CloseRequests != 0 {
		t.Fatalf("publication without merge closed the issue: %+v", state)
	}

	// Human merges the published revision. This action does not close the issue
	// in the fixture; only the explicit GH issue close command can do that.
	git(repo.Path, "fetch", "origin", state.Impl.Branch)
	switch strategy {
	case "merge":
		git(repo.Path, "merge", "--no-ff", record.HeadSHA, "-m", "Human merges greeting implementation")
	case "squash":
		git(repo.Path, "merge", "--squash", record.HeadSHA)
		git(repo.Path, "commit", "-m", "Squash verified greeting")
	case "rebase":
		git(repo.Path, "commit", "--allow-empty", "-m", "Advance base after verification")
		git(repo.Path, "checkout", "--detach", record.HeadSHA)
		git(repo.Path, "rebase", "main")
		rebased := git(repo.Path, "rev-parse", "HEAD")
		git(repo.Path, "checkout", "main")
		git(repo.Path, "merge", "--ff-only", rebased)
	}
	git(repo.Path, "push", "origin", "main")
	state.Impl.MergeSHA = git(origin, "rev-parse", "refs/heads/main")
	if strategy != "merge" && state.Impl.MergeSHA == record.HeadSHA {
		t.Fatalf("%s fixture did not rewrite the verified commit", strategy)
	}
	verifiedDeliveryWriteState(t, statePath, state)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := openDB()
	restartedRepos := storage.NewRepositories(restarted.DB())
	for pass := range 2 {
		// Fresh runner and gateway each time; no in-memory reconcile state survives.
		if err := newCoordinator(restartedRepos).ReconcileDeliveries(ctx, coordInput); err != nil {
			t.Fatalf("reconcile after restart, pass %d: %v", pass, err)
		}
		state = verifiedDeliveryReadState(t, statePath)
		if state.IssueState != "closed" || state.IssueReason != "completed" || state.CloseRequests != 1 || state.PRCreates != 1 {
			t.Fatalf("reconcile pass %d: want exactly one completed close and one implementation PR, got %+v", pass, state)
		}
		if !slices.Equal(state.IssueLabels, []string{"triaged"}) {
			t.Fatalf("reconcile pass %d: want phase labels removed and triage label preserved, got %v", pass, state.IssueLabels)
		}
		stored, err := restartedRepos.Loops.GetByID(ctx, result.LoopID)
		if err != nil || stored == nil {
			t.Fatalf("reload settled Worker: %v", err)
		}
		var meta struct {
			ImplementationMerged struct{ MergeSHA string }
			DeliveryReconcile    struct{ Done, IssueClosed bool }
		}
		if err := json.Unmarshal([]byte(derefString(stored.MetadataJSON)), &meta); err != nil || meta.ImplementationMerged.MergeSHA != state.Impl.MergeSHA || !meta.DeliveryReconcile.Done || !meta.DeliveryReconcile.IssueClosed {
			t.Fatalf("reconcile pass %d: missing durable confirmation of merge and closure: %+v, %v", pass, meta, err)
		}
	}
	t.Log("after restart, coordinator closed the issue as completed exactly once")
}

type verifiedDeliveryPR struct {
	Number                                          int
	Title, Body, Branch, BaseSHA, HeadSHA, MergeSHA string
	Labels                                          []string
}

type verifiedDeliveryGHState struct {
	Origin, GitPath, IssueState, IssueReason string
	IssueLabels                              []string
	CloseRequests, PRCreates                 int
	Spec                                     verifiedDeliveryPR
	Impl                                     *verifiedDeliveryPR
}

func verifiedDeliveryReadState(t *testing.T, path string) verifiedDeliveryGHState {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var state verifiedDeliveryGHState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func verifiedDeliveryWriteState(t *testing.T, path string, state verifiedDeliveryGHState) {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	// All mutations are serial; concurrent discovery subprocesses only read.
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestVerifiedDeliveryFakeGH is the subprocess entry point for the fixture.
// Unknown commands fail closed, and no handler contacts GitHub.
func TestVerifiedDeliveryFakeGH(t *testing.T) {
	sep := slices.Index(os.Args, "--")
	if sep < 0 || os.Getenv("LOOPER_VERIFIED_DELIVERY_GH_STATE") == "" {
		return
	}
	args := os.Args[sep+1:]
	path := os.Getenv("LOOPER_VERIFIED_DELIVERY_GH_STATE")
	s := verifiedDeliveryReadState(t, path)
	git := func(args ...string) string {
		t.Helper()
		out, err := exec.Command(s.GitPath, append([]string{"--git-dir", s.Origin}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("fake GH git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	flag := func(key string) string {
		if i := slices.Index(args, key); i >= 0 && i+1 < len(args) {
			return args[i+1]
		}
		return ""
	}
	labels := func(names []string) []map[string]string {
		rows := make([]map[string]string, 0, len(names))
		for _, name := range names {
			rows = append(rows, map[string]string{"name": name})
		}
		return rows
	}
	issue := func() map[string]any {
		return map[string]any{"number": verifiedDeliveryIssue, "title": verifiedDeliveryTitle, "body": "Add greeting.sh that prints hello, delivery.",
			"url": verifiedDeliveryURL + "/issues/42", "html_url": verifiedDeliveryURL + "/issues/42", "state": s.IssueState, "state_reason": s.IssueReason,
			"updatedAt": verifiedDeliveryTime, "updated_at": verifiedDeliveryTime, "created_at": verifiedDeliveryTime, "labels": labels(s.IssueLabels),
			"author": map[string]string{"login": "human"}, "user": map[string]string{"login": "human"}, "assignees": []any{}}
	}
	prURL := func(p *verifiedDeliveryPR) string { return fmt.Sprintf("%s/pull/%d", verifiedDeliveryURL, p.Number) }
	prFiles := func(p *verifiedDeliveryPR) []string {
		return strings.Fields(git("diff", "--name-only", p.BaseSHA, p.HeadSHA))
	}
	prRow := func(p *verifiedDeliveryPR) map[string]any {
		state, cliState, mergedAt := "open", "OPEN", ""
		if p.MergeSHA != "" {
			state, cliState, mergedAt = "closed", "MERGED", verifiedDeliveryTime
		}
		return map[string]any{"number": p.Number, "title": p.Title, "body": p.Body, "url": prURL(p), "html_url": prURL(p),
			"state": state, "cliState": cliState, "merged": p.MergeSHA != "", "merged_at": mergedAt, "merge_commit_sha": p.MergeSHA, "changed_files": len(prFiles(p)),
			"headRefName": p.Branch, "baseRefName": "main", "headRefOid": p.HeadSHA, "baseRefOid": p.BaseSHA, "labels": labels(p.Labels),
			"head": map[string]any{"sha": p.HeadSHA, "ref": p.Branch, "repo": map[string]string{"full_name": verifiedDeliveryRepo}},
			"base": map[string]any{"sha": p.BaseSHA, "ref": "main", "repo": map[string]string{"full_name": verifiedDeliveryRepo}}}
	}
	emit := func(value any) {
		if err := json.NewEncoder(os.Stdout).Encode(value); err != nil {
			t.Fatal(err)
		}
		os.Exit(0)
	}
	if len(args) == 0 {
		t.Fatal("expected fake GH command")
	}
	if len(args) >= 2 {
		switch strings.Join(args[:2], " ") {
		case "issue list":
			for i, arg := range args {
				if arg == "--label" && !slices.Contains(s.IssueLabels, args[i+1]) {
					emit([]any{})
				}
			}
			if s.IssueState != "open" {
				emit([]any{})
			}
			emit([]any{issue()})
		case "issue close":
			if len(args) < 3 || args[2] != fmt.Sprint(verifiedDeliveryIssue) || flag("--repo") != verifiedDeliveryRepo || flag("--reason") != "completed" {
				t.Fatalf("invalid issue close: %q", args)
			}
			s.IssueState = "closed"
			s.IssueReason = flag("--reason")
			s.CloseRequests++
			verifiedDeliveryWriteState(t, path, s)
			emit(map[string]any{})
		case "label create":
			emit(map[string]any{})
		case "pr list":
			if s.Impl != nil && s.Impl.MergeSHA == "" {
				row := prRow(s.Impl)
				row["state"] = row["cliState"]
				emit([]any{row})
			}
			emit([]any{})
		case "pr create":
			if s.Impl != nil || flag("--repo") != verifiedDeliveryRepo || flag("--base") != "main" || flag("--head") == "" || flag("--title") == "" {
				t.Fatalf("invalid implementation PR creation: %v", args)
			}
			branch := flag("--head")
			s.Impl = &verifiedDeliveryPR{Number: verifiedDeliveryImplPR, Title: flag("--title"), Body: flag("--body"), Branch: branch, BaseSHA: git("rev-parse", "refs/heads/main"), HeadSHA: git("rev-parse", "refs/heads/"+branch)}
			s.PRCreates++
			verifiedDeliveryWriteState(t, path, s)
			fmt.Fprintln(os.Stdout, prURL(s.Impl))
			os.Exit(0)
		case "pr view":
			for _, p := range []*verifiedDeliveryPR{&s.Spec, s.Impl} {
				if p != nil && args[2] == fmt.Sprint(p.Number) {
					row := prRow(p)
					if flag("--jq") == ".state" {
						fmt.Fprintln(os.Stdout, row["cliState"])
						os.Exit(0)
					}
					row["state"] = row["cliState"]
					emit(row)
				}
			}
		case "pr edit":
			if s.Impl != nil && args[2] == fmt.Sprint(s.Impl.Number) {
				if body := flag("--body"); body != "" {
					s.Impl.Body = body
				}
				if title := flag("--title"); title != "" {
					s.Impl.Title = title
				}
				verifiedDeliveryWriteState(t, path, s)
				emit(map[string]any{})
			}
		}
	}
	if args[0] == "api" {
		var route string
		var query url.Values
		for _, arg := range args[1:] {
			if strings.HasPrefix(strings.TrimPrefix(arg, "/"), "repos/") || arg == "user" {
				endpoint, err := url.Parse(strings.TrimPrefix(arg, "/"))
				if err != nil {
					t.Fatal(err)
				}
				route, query = endpoint.EscapedPath(), endpoint.Query()
				break
			}
		}
		method := flag("--method")
		switch {
		case route == "user":
			if flag("--jq") == ".login" {
				fmt.Fprintln(os.Stdout, "worker-bot")
				os.Exit(0)
			}
			emit(map[string]string{"login": "worker-bot"})
		case route == verifiedDeliveryIssueRoute && (method == "" || method == "GET"):
			if flag("--jq") == ".state" {
				fmt.Fprintln(os.Stdout, s.IssueState)
				os.Exit(0)
			}
			emit(issue())
		case route == verifiedDeliveryIssueRoute+"/labels" && method == "POST":
			for _, arg := range args {
				if label, ok := strings.CutPrefix(arg, "labels[]="); ok && !slices.Contains(s.IssueLabels, label) {
					s.IssueLabels = append(s.IssueLabels, label)
				}
			}
			verifiedDeliveryWriteState(t, path, s)
			emit(labels(s.IssueLabels))
		case strings.HasPrefix(route, verifiedDeliveryIssueRoute+"/labels/") && method == "DELETE":
			label, err := url.PathUnescape(strings.TrimPrefix(route, verifiedDeliveryIssueRoute+"/labels/"))
			if err != nil {
				t.Fatal(err)
			}
			s.IssueLabels = slices.DeleteFunc(s.IssueLabels, func(name string) bool { return name == label })
			verifiedDeliveryWriteState(t, path, s)
			emit(map[string]any{})
		case route == verifiedDeliveryIssueRoute+"/comments" && method == "POST":
			emit(map[string]any{"id": 1, "html_url": verifiedDeliveryURL + "/issues/42#issuecomment-1"})
		case route == verifiedDeliveryIssueRoute+"/comments":
			emit([][]any{{}})
		case strings.HasPrefix(route, "repos/"+verifiedDeliveryRepo+"/git/ref/heads/"):
			ref, err := url.PathUnescape(strings.TrimPrefix(route, "repos/"+verifiedDeliveryRepo+"/git/ref/heads/"))
			if err != nil {
				t.Fatal(err)
			}
			ref = "refs/heads/" + ref
			emit(map[string]any{"ref": ref, "object": map[string]string{"type": "commit", "sha": git("rev-parse", ref)}})
		case strings.HasPrefix(route, "repos/"+verifiedDeliveryRepo+"/contents/"):
			file, err := url.PathUnescape(strings.TrimPrefix(route, "repos/"+verifiedDeliveryRepo+"/contents/"))
			if err != nil || query.Get("ref") == "" {
				t.Fatalf("bad contents endpoint: %q", args)
			}
			object := query.Get("ref") + ":" + file
			emit(map[string]string{"path": file, "type": "file", "sha": git("rev-parse", object)})
		case strings.HasPrefix(route, "repos/"+verifiedDeliveryRepo+"/compare/"):
			refs := strings.TrimPrefix(route, "repos/"+verifiedDeliveryRepo+"/compare/")
			refs, err := url.PathUnescape(refs)
			if err != nil {
				t.Fatal(err)
			}
			base, head, ok := strings.Cut(refs, "...")
			if !ok {
				t.Fatalf("invalid compare ref %q", refs)
			}
			ahead := json.Number(git("rev-list", "--count", base+".."+head))
			behind := json.Number(git("rev-list", "--count", head+".."+base))
			status := "diverged"
			switch {
			case ahead == "0" && behind == "0":
				status = "identical"
			case behind == "0":
				status = "ahead"
			case ahead == "0":
				status = "behind"
			}
			switch flag("--jq") {
			case "{status: .status, behind_by: .behind_by, merge_base: .merge_base_commit.sha}":
				emit(map[string]any{"status": status, "behind_by": behind, "merge_base": git("merge-base", base, head)})
			case "":
				emit(map[string]any{"ahead_by": ahead, "behind_by": behind, "total_commits": ahead, "status": status})
			}
		}
		for _, p := range []*verifiedDeliveryPR{&s.Spec, s.Impl} {
			if p == nil {
				continue
			}
			prRoute := fmt.Sprintf("repos/%s/pulls/%d", verifiedDeliveryRepo, p.Number)
			switch route {
			case prRoute:
				emit(prRow(p))
			case prRoute + "/files":
				if query.Get("per_page") != "100" || query.Get("page") != "1" {
					t.Fatalf("expected single page of PR files: %q", args)
				}
				files := []map[string]string{}
				for _, name := range prFiles(p) {
					files = append(files, map[string]string{"filename": name, "status": "added"})
				}
				emit(files)
			}
		}
	}
	t.Fatalf("unexpected fake GH invocation: %q", args)
}
