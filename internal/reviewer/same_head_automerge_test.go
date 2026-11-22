package reviewer

import (
	"context"
	"testing"
	"time"

	"github.com/nexu-io/looper/internal/config"
	githubinfra "github.com/nexu-io/looper/internal/infra/github"
	"github.com/nexu-io/looper/internal/loops"
	"github.com/nexu-io/looper/internal/storage"
)

func TestSameHeadAutoMergeAfterLabelChange(t *testing.T) {
	for _, tt := range []struct {
		name   string
		direct bool
		legacy bool
		noop   bool
		ci     bool
		mutate func(*testing.T, *fakeGitHubGateway, map[string]any)
		clear  func(*fakeGitHubGateway)
		want   bool
	}{
		{name: "waiting discovery", want: true},
		{name: "old review and conversation are not fresh blockers", want: true},
		{name: "legacy exact marker retry", legacy: true, direct: true, want: true},
		{name: "recorded clean no-op", noop: true, direct: true, want: true},
		{name: "direct merge waits for CI", direct: true, ci: true, want: true},
		{name: "legacy no-op is ambiguous", noop: true, legacy: true},
		{name: "no positive marker", legacy: true, mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) { g.reviews = nil }},
		{name: "comment is nonclean", legacy: true, mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) {
			g.reviews[0]["body"] = appendReviewMarker("blocking", agentNativeReviewMarker("same-head", "abc123", "reviewer:same-head:abc123"), "blocking")
			g.reviews[0]["state"] = "COMMENTED"
		}},
		{name: "no publication identity", mutate: func(_ *testing.T, _ *fakeGitHubGateway, m map[string]any) {
			m[metadataLastPublishedReviewIDKey] = ""
			m["lastReviewEvent"] = "COMMENT"
		}},
		{name: "deferred scope hold", mutate: func(t *testing.T, _ *fakeGitHubGateway, m map[string]any) {
			encoded, err := mergeLoopMetadataJSON(nil, m)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err = loops.PersistPendingReviewScopeHumanEvidence(&encoded, "Is this in scope?", "", true)
			if err != nil {
				t.Fatal(err)
			}
			for key, value := range parseJSONObject(&encoded) {
				m[key] = value
			}
		}},
		{name: "live PR hold", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) { g.labels = append(g.labels, "looper:hold") }},
		{name: "linked issue hold", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) {
			g.issueDetail.Labels = append(g.issueDetail.Labels, "looper:hold")
		}, clear: func(g *fakeGitHubGateway) { g.issueDetail.Labels = []string{"triaged", "dispatch/implement"} }},
		{name: "repository policy refusal", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) {
			g.repositorySettings = githubinfra.RepositorySettings{AllowSquashMerge: true, AllowAutoMerge: false}
		}, clear: func(g *fakeGitHubGateway) { g.repositorySettings.AllowAutoMerge = true }},
		{name: "unresolved human COMMENT thread", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) {
			g.reviews = append(g.reviews, map[string]any{"id": "human-review", "state": "COMMENTED", "author": map[string]any{"login": "alice"}})
			g.reviewThreads = []ReviewThread{{ID: "human-thread", Comments: []ReviewThreadComment{{Author: "alice", Body: "Please fix this before merging."}}}}
		}, clear: func(g *fakeGitHubGateway) { g.reviewThreads[0].IsResolved = true }},
		{name: "fresh blocking COMMENT review body", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) {
			g.reviews = append(g.reviews, map[string]any{"id": "human-review", "state": "COMMENTED", "body": "Please do not merge yet.", "author": map[string]any{"login": "alice"}})
		}, clear: func(g *fakeGitHubGateway) { g.reviews[len(g.reviews)-1]["state"] = "DISMISSED" }},
		{name: "fresh PR conversation blocker", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) {
			g.issueComments = append(g.issueComments, map[string]any{"id": int64(42), "body": "Please do not merge yet.", "updatedAt": "2099-01-01T00:00:00Z"})
		}, clear: func(g *fakeGitHubGateway) { g.issueComments = g.issueComments[:len(g.issueComments)-1] }},
		{name: "draft", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) { g.viewDraft = true }},
		{name: "spec", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) {
			g.labels = append(g.labels, "looper:spec-reviewing")
		}},
		{name: "head changes after filter", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) {
			g.viewCalls = 0
			g.changeHeadOnSecondView = true
		}},
		{name: "closes before mutation", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) { g.closeAfterReviewMarker = true }},
		{name: "changes requested", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) { g.reviewDecision = "CHANGES_REQUESTED" }},
		{name: "new criteria were never verified", mutate: func(_ *testing.T, g *fakeGitHubGateway, _ map[string]any) {
			g.issueDetail.Body = "## Acceptance criteria\n- New requirement\n"
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			fixture := newRunnerFixture(t)
			g := &fakeGitHubGateway{author: "octocat", currentLogin: "reviewer", labels: []string{"team:backend"}, reviewRequests: []string{"reviewer"}, reviewMarkerMissing: true,
				viewBody: "Implements feature.\n\nCloses #358", issueDetail: githubinfra.IssueDetail{Number: 358, Labels: []string{"triaged", "dispatch/implement"}}}
			agent := &fakeAgentExecutor{results: []AgentResult{{Status: "completed", Summary: "No actionable findings", Stdout: `__LOOPER_RESULT__={"summary":"No actionable findings"}`, ParseStatus: "parsed"}}}
			cfg := reviewerAutoMergeTestConfig(t)
			cfg.Roles.Reviewer.AutoMerge.DirectMerge = tt.direct
			cleanEvent, wantSubmits := config.ReviewerReviewEventApprove, 1
			if tt.noop {
				cleanEvent, wantSubmits = config.ReviewerReviewEventComment, 0
				cfg.Roles.Reviewer.Behavior.ReviewEvents.Clean = cleanEvent
			}
			now := fixture.now()
			if tt.name == "old review and conversation are not fresh blockers" {
				older := now.Add(-time.Hour).Format(time.RFC3339)
				g.reviews = []map[string]any{{"id": "earlier-review", "state": "COMMENTED", "body": "Please fix the earlier revision.", "submittedAt": older}}
				g.issueComments = []map[string]any{{"id": int64(2), "body": "Please wait for the earlier review.", "updatedAt": older}}
			}
			loopCfg := testReviewerLoopConfig()
			loopCfg.StopOnApproved = true
			runner := New(Options{DB: fixture.coordinator.DB(), Repos: fixture.repos, GitHub: g, Git: &fakeGitGateway{}, AgentExecutor: agent, Logger: fixture.logger,
				Now: func() time.Time { return now }, ReviewEvents: config.ReviewerReviewEventsConfig{Clean: cleanEvent}, LoopConfig: loopCfg, CustomInstructions: cfg})
			repo, pr := "acme/looper", int64(42)
			meta := `{"followUpdates":true,"loop":{"enabled":true}}`
			loop := storage.LoopRecord{ID: "same-head", Seq: 1, ProjectID: "project_1", Type: "reviewer", TargetType: "pull_request", Repo: &repo, PRNumber: &pr, Status: "queued", MetadataJSON: &meta, CreatedAt: runner.nowISO(), UpdatedAt: runner.nowISO()}
			if err := fixture.repos.Loops.Upsert(ctx, loop); err != nil {
				t.Fatal(err)
			}
			enqueue := func() {
				t.Helper()
				if _, err := runner.enqueue(ctx, enqueueInput{ProjectID: loop.ProjectID, LoopID: loop.ID, Repo: repo, PRNumber: pr, HeadSHA: "abc123"}); err != nil {
					t.Fatal(err)
				}
			}
			process := func() *ProcessResult {
				t.Helper()
				result, err := runner.ProcessNext(ctx, "test-reviewer")
				if err != nil || result == nil {
					t.Fatalf("ProcessNext = (%#v, %v)", result, err)
				}
				return result
			}
			enqueue()
			if result := process(); result.Status != "success" || len(g.submitReviewCalls) != wantSubmits || len(g.enableAutoMergeCalls)+len(g.mergePullRequestCalls) != 0 {
				t.Fatalf("initial unlabelled review: %#v; submits = %v", result, g.submitReviewCalls)
			}
			live, err := fixture.repos.Loops.GetByID(ctx, loop.ID)
			if err != nil || live == nil || live.Status != "waiting" {
				t.Fatalf("waiting loop = (%#v, %v)", live, err)
			}
			count, comments := loops.ReviewerPublishCount(live.MetadataJSON), len(g.issueCommentCalls)
			g.labels = []string{"looper:worker-ready"}
			g.reviewRequests = []string{}
			if tt.legacy || tt.mutate != nil {
				m := parseJSONObject(live.MetadataJSON)
				if tt.legacy {
					delete(m, metadataMergeProof)
				}
				if tt.mutate != nil {
					tt.mutate(t, g, m)
				}
				meta, err := mergeLoopMetadataJSON(nil, m)
				if err != nil {
					t.Fatal(err)
				}
				live.MetadataJSON = &meta
				if err := fixture.repos.Loops.Upsert(ctx, *live); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(time.Hour)
			discover := func(want int) DiscoveryResult {
				t.Helper()
				result, err := runner.DiscoverPullRequest(ctx, TargetedDiscoveryInput{ProjectID: loop.ProjectID, Repo: repo, PRNumber: pr})
				if err != nil || len(result.QueueItems) != want {
					t.Fatalf("discovery = (%#v, %v), want %d queue items", result, err, want)
				}
				return result
			}
			if tt.name == "waiting discovery" || tt.clear != nil {
				discover(1)
				now = now.Add(time.Hour)
			} else {
				enqueue()
			}
			if tt.ci {
				g.viewChecksSummary = "SUCCESS, IN_PROGRESS"
				if result := process(); result.FailureKind != FailureRetryableAfterResume || len(g.mergePullRequestCalls) != 0 {
					t.Fatalf("pending CI: %#v; merges = %v", result, g.mergePullRequestCalls)
				}
				g.viewChecksSummary = "SUCCESS"
				now = now.Add(time.Hour)
			}
			if result := process(); result.Status != "skipped" {
				t.Fatalf("reconciliation: %#v", result)
			}
			if tt.clear != nil {
				if len(g.enableAutoMergeCalls)+len(g.mergePullRequestCalls) != 0 {
					t.Fatal("merge attempted while blocker was active")
				}
				tt.clear(g)
				queued := discover(1).QueueItems[0]
				quiet := time.Duration(loopCfg.QuietPeriodSeconds) * time.Second
				if at := parseRFC3339OrZero(queued.AvailableAt); !at.Equal(now.Add(quiet)) {
					t.Fatalf("reconciliation scheduled for %s, want %s", at, now.Add(quiet))
				}
				now = now.Add(quiet / 2)
				if again := discover(1).QueueItems[0]; again.ID != queued.ID || again.AvailableAt != queued.AvailableAt {
					t.Fatalf("repeat discovery changed queued reconciliation: before=%#v after=%#v", queued, again)
				}
				if result, err := runner.ProcessNext(ctx, "test-reviewer"); err != nil || result != nil {
					t.Fatalf("reconciliation ran before quiet period ended: %#v, %v", result, err)
				}
				now = now.Add(quiet / 2)
				if result := process(); result.Status != "skipped" {
					t.Fatalf("retry reconciliation: %#v", result)
				}
			}
			var heads []string
			for _, call := range g.enableAutoMergeCalls {
				heads = append(heads, call.HeadSHA)
			}
			for _, call := range g.mergePullRequestCalls {
				heads = append(heads, call.HeadSHA)
			}
			wantMerge := tt.want || tt.clear != nil
			if wantMerge && (len(heads) != 1 || heads[0] != "abc123") || !wantMerge && len(heads) != 0 {
				t.Fatalf("merge heads = %v, want pinned merge: %v", heads, wantMerge)
			}
			live, err = fixture.repos.Loops.GetByID(ctx, loop.ID)
			if err != nil || live == nil {
				t.Fatalf("final loop = (%#v, %v)", live, err)
			}
			if len(agent.starts) != 1 || len(g.submitReviewCalls) != wantSubmits || len(g.issueCommentCalls) != comments || loops.ReviewerPublishCount(live.MetadataJSON) != count {
				t.Fatalf("reconciliation ran the agent or published again: agent=%d reviews=%d comments=%d count=%d", len(agent.starts), len(g.submitReviewCalls), len(g.issueCommentCalls), loops.ReviewerPublishCount(live.MetadataJSON))
			}
			if wantMerge {
				discover(0)
			}
		})
	}
}
