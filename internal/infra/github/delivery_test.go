package github

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/nexu-io/looper/internal/delivery"
	"github.com/nexu-io/looper/internal/infra/shell"
)

const (
	deliveryHeadSHA  = "1111111111111111111111111111111111111111"
	deliveryMergeSHA = "2222222222222222222222222222222222222222"
	deliveryTipSHA   = "3333333333333333333333333333333333333333"
	deliveryBlobSHA  = "4444444444444444444444444444444444444444"
	deliveryOtherSHA = "5555555555555555555555555555555555555555"
)

func deliveryPRJSON(state string, merged bool, mergeSHA, headRepo string, changedFiles int) string {
	headRepoJSON := "null"
	if headRepo != "" {
		headRepoJSON = fmt.Sprintf(`{"full_name":%q}`, headRepo)
	}
	return fmt.Sprintf(`{"number":7,"state":%q,"merged":%t,"merge_commit_sha":%q,"changed_files":%d,
"base":{"ref":"main","sha":"aaaa","repo":{"full_name":"Acme/Looper"}},
"head":{"ref":"looper/issue-7","sha":%q,"repo":%s}}`, state, merged, mergeSHA, changedFiles, deliveryHeadSHA, headRepoJSON)
}

func notFoundResult() (shell.Result, error) {
	result := shell.Result{ExitCode: 1, Stderr: "gh: Not Found (HTTP 404)"}
	return result, &shell.CommandExecutionError{Message: "Command exited with code 1", Result: result}
}

func newDeliveryGateway(t *testing.T, routes map[string]func() (shell.Result, error)) (*Gateway, *fakeGHRunner) {
	t.Helper()
	runner := &fakeGHRunner{t: t}
	runner.respond = func(options shell.Options) (shell.Result, error) {
		if len(options.Args) < 2 || options.Args[0] != "api" {
			t.Fatalf("unexpected gh args: %q", strings.Join(options.Args, " "))
		}
		route, ok := routes[options.Args[1]]
		if !ok {
			t.Fatalf("unexpected gh api endpoint: %q", options.Args[1])
		}
		return route()
	}
	return New(Options{GHPath: "gh", CWD: t.TempDir(), GHRun: runner.run}), runner
}

func ok(stdout string) func() (shell.Result, error) {
	return func() (shell.Result, error) { return shell.Result{Stdout: stdout}, nil }
}

const (
	deliveryPREndpoint      = "repos/acme/looper/pulls/7"
	deliveryBranchEndpoint  = "repos/acme/looper/git/ref/heads/main"
	deliveryCompareEndpoint = "repos/acme/looper/compare/" + deliveryMergeSHA + "..." + deliveryTipSHA
	deliveryTipRef          = `{"ref":"refs/heads/main","object":{"type":"commit","sha":"` + deliveryTipSHA + `"}}`
	deliveryContainsJSON    = `{"status":"ahead","behind_by":0,"merge_base":"` + deliveryMergeSHA + `"}`
)

func TestDeliveryPullRequestMergedVariantsUseCanonicalMergeCommit(t *testing.T) {
	t.Parallel()
	// Merge, squash, and rebase all surface the commit GitHub wrote to the base
	// as merge_commit_sha; the original head is never required on base.
	for _, mergeKind := range []string{"merge", "squash", "rebase"} {
		mergeKind := mergeKind
		t.Run(mergeKind, func(t *testing.T) {
			t.Parallel()
			gateway, runner := newDeliveryGateway(t, map[string]func() (shell.Result, error){
				deliveryPREndpoint:      ok(deliveryPRJSON("closed", true, deliveryMergeSHA, "acme/looper", 2)),
				deliveryBranchEndpoint:  ok(deliveryTipRef),
				deliveryCompareEndpoint: ok(deliveryContainsJSON),
			})
			got, err := gateway.DeliveryPullRequest(context.Background(), "acme/looper", 7, "")
			if err != nil {
				t.Fatalf("DeliveryPullRequest() error = %v", err)
			}
			want := delivery.PullRequest{Number: 7, State: delivery.PullRequestMerged, BaseBranch: "main", HeadSHA: deliveryHeadSHA, MergeSHA: deliveryMergeSHA, Repo: "acme/looper", HeadBranch: "looper/issue-7", BaseContainsMerge: true}
			if got != want {
				t.Fatalf("DeliveryPullRequest() = %#v, want %#v", got, want)
			}
			for _, call := range runner.calls {
				if strings.Contains(call, deliveryHeadSHA) {
					t.Fatalf("containment must not depend on original head: %q", call)
				}
			}
		})
	}
}

func TestDeliveryPullRequestMergeNotOnBaseTip(t *testing.T) {
	t.Parallel()
	for name, compare := range map[string]string{
		"diverged": `{"status":"diverged","behind_by":3,"merge_base":"` + deliveryOtherSHA + `"}`,
		"behind":   `{"status":"behind","behind_by":1,"merge_base":"` + deliveryTipSHA + `"}`,
	} {
		compare := compare
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gateway, _ := newDeliveryGateway(t, map[string]func() (shell.Result, error){
				deliveryPREndpoint:      ok(deliveryPRJSON("closed", true, deliveryMergeSHA, "acme/looper", 2)),
				deliveryBranchEndpoint:  ok(deliveryTipRef),
				deliveryCompareEndpoint: ok(compare),
			})
			got, err := gateway.DeliveryPullRequest(context.Background(), "acme/looper", 7, "")
			if err != nil || got.State != delivery.PullRequestMerged || got.BaseContainsMerge {
				t.Fatalf("DeliveryPullRequest() = %#v, %v; want merged without base containment", got, err)
			}
		})
	}
}

func TestDeliveryPullRequestInconsistentCompareIsError(t *testing.T) {
	t.Parallel()
	gateway, _ := newDeliveryGateway(t, map[string]func() (shell.Result, error){
		deliveryPREndpoint:      ok(deliveryPRJSON("closed", true, deliveryMergeSHA, "acme/looper", 2)),
		deliveryBranchEndpoint:  ok(deliveryTipRef),
		deliveryCompareEndpoint: ok(`{"status":"ahead","behind_by":0,"merge_base":"` + deliveryOtherSHA + `"}`),
	})
	if _, err := gateway.DeliveryPullRequest(context.Background(), "acme/looper", 7, ""); err == nil {
		t.Fatal("DeliveryPullRequest() error = nil, want inconsistent compare error")
	}
}

func TestDeliveryPullRequestOpenAndClosedStates(t *testing.T) {
	t.Parallel()
	for state, want := range map[string]string{"open": delivery.PullRequestOpen, "closed": delivery.PullRequestClosed} {
		state, want := state, want
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			gateway, runner := newDeliveryGateway(t, map[string]func() (shell.Result, error){
				deliveryPREndpoint: ok(deliveryPRJSON(state, false, "", "acme/looper", 1)),
			})
			got, err := gateway.DeliveryPullRequest(context.Background(), "acme/looper", 7, "")
			if err != nil || got.State != want || got.MergeSHA != "" || got.BaseContainsMerge {
				t.Fatalf("DeliveryPullRequest() = %#v, %v; want state %s", got, err, want)
			}
			if len(runner.calls) != 1 {
				t.Fatalf("calls = %#v, want PR read only", runner.calls)
			}
		})
	}
}

func TestDeliveryPullRequestReportsUpdatedHead(t *testing.T) {
	t.Parallel()
	head := deliveryHeadSHA
	gateway, _ := newDeliveryGateway(t, map[string]func() (shell.Result, error){
		deliveryPREndpoint: func() (shell.Result, error) {
			return shell.Result{Stdout: strings.Replace(deliveryPRJSON("open", false, "", "acme/looper", 1), deliveryHeadSHA, head, 1)}, nil
		},
	})
	first, err := gateway.DeliveryPullRequest(context.Background(), "acme/looper", 7, "")
	if err != nil || first.HeadSHA != deliveryHeadSHA {
		t.Fatalf("first DeliveryPullRequest() = %#v, %v", first, err)
	}
	head = deliveryOtherSHA
	second, err := gateway.DeliveryPullRequest(context.Background(), "acme/looper", 7, "")
	if err != nil || second.HeadSHA != deliveryOtherSHA {
		t.Fatalf("second DeliveryPullRequest() = %#v, %v; want updated head", second, err)
	}
}

func TestDeliveryPullRequestUnknownIsErrorNotClosed(t *testing.T) {
	t.Parallel()
	gateway, _ := newDeliveryGateway(t, map[string]func() (shell.Result, error){deliveryPREndpoint: notFoundResult})
	got, err := gateway.DeliveryPullRequest(context.Background(), "acme/looper", 7, "")
	if err == nil || !IsNotFoundError(err) || got.State != "" {
		t.Fatalf("DeliveryPullRequest() = %#v, %v; want not-found error", got, err)
	}
}

func TestDeliveryPullRequestRejectsWrongIdentity(t *testing.T) {
	t.Parallel()
	base := deliveryPRJSON("closed", true, deliveryMergeSHA, "acme/looper", 1)
	cases := map[string]string{
		"fork head":         deliveryPRJSON("closed", true, deliveryMergeSHA, "mallory/looper", 1),
		"deleted head repo": deliveryPRJSON("closed", true, deliveryMergeSHA, "", 1),
		"other base repo":   strings.Replace(base, `"Acme/Looper"`, `"acme/other"`, 1),
		"other number":      strings.Replace(base, `"number":7`, `"number":8`, 1),
		"bad merge sha":     deliveryPRJSON("closed", true, "abc", "acme/looper", 1),
		"merged but open":   deliveryPRJSON("open", true, deliveryMergeSHA, "acme/looper", 1),
		"unknown merged":    strings.Replace(base, `"merged":true`, `"merged":null`, 1),
		"unknown state":     deliveryPRJSON("draft", false, "", "acme/looper", 1),
		"bad head sha":      strings.Replace(base, deliveryHeadSHA, "HEAD", 1),
		"traversal branch":  strings.Replace(base, `"ref":"main"`, `"ref":"../main"`, 1),
	}
	for name, payload := range cases {
		payload := payload
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gateway, _ := newDeliveryGateway(t, map[string]func() (shell.Result, error){deliveryPREndpoint: ok(payload)})
			if got, err := gateway.DeliveryPullRequest(context.Background(), "acme/looper", 7, ""); err == nil {
				t.Fatalf("DeliveryPullRequest() = %#v, want identity error", got)
			}
		})
	}
}

func TestDeliveryPullRequestRejectsInvalidInputWithoutCalls(t *testing.T) {
	t.Parallel()
	gateway, runner := newDeliveryGateway(t, nil)
	for _, repo := range []string{"", "acme", "acme/../x", "acme/looper/extra/x", "../looper"} {
		if _, err := gateway.DeliveryPullRequest(context.Background(), repo, 7, ""); err == nil {
			t.Fatalf("DeliveryPullRequest(%q) error = nil", repo)
		}
	}
	if _, err := gateway.DeliveryPullRequest(context.Background(), "acme/looper", 0, ""); err == nil {
		t.Fatal("DeliveryPullRequest(pr=0) error = nil")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("calls = %#v, want none", runner.calls)
	}
}

func TestDeliveryPullRequestHostnameRepo(t *testing.T) {
	t.Parallel()
	runner := &fakeGHRunner{t: t}
	runner.respond = func(options shell.Options) (shell.Result, error) {
		args := strings.Join(options.Args, " ")
		if !strings.HasSuffix(args, "--hostname ghe.example.com") {
			t.Fatalf("missing hostname: %q", args)
		}
		return shell.Result{Stdout: deliveryPRJSON("open", false, "", "acme/looper", 1)}, nil
	}
	gateway := New(Options{GHPath: "gh", GHRun: runner.run})
	got, err := gateway.DeliveryPullRequest(context.Background(), "ghe.example.com/acme/looper", 7, "")
	if err != nil || got.Repo != "ghe.example.com/acme/looper" {
		t.Fatalf("DeliveryPullRequest() = %#v, %v", got, err)
	}
}

const (
	deliverySpecPath        = "docs/specs/issue 7.md"
	deliveryContentsAtMerge = "repos/acme/looper/contents/docs/specs/issue%207.md?ref=" + deliveryMergeSHA
	deliveryContentsAtTip   = "repos/acme/looper/contents/docs/specs/issue%207.md?ref=" + deliveryTipSHA
)

func deliveryFileJSON(blob string) string {
	return fmt.Sprintf(`{"type":"file","path":%q,"sha":%q,"content":"aGk="}`, deliverySpecPath, blob)
}

func TestDeliverySpecOnBase(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		compare string
		merge   func() (shell.Result, error)
		tip     func() (shell.Result, error)
		want    bool
		wantErr bool
	}{
		{name: "same blob", compare: deliveryContainsJSON, merge: ok(deliveryFileJSON(deliveryBlobSHA)), tip: ok(deliveryFileJSON(deliveryBlobSHA)), want: true},
		{name: "blob changed on base", compare: deliveryContainsJSON, merge: ok(deliveryFileJSON(deliveryBlobSHA)), tip: ok(deliveryFileJSON(deliveryOtherSHA))},
		{name: "removed from base", compare: deliveryContainsJSON, merge: ok(deliveryFileJSON(deliveryBlobSHA)), tip: notFoundResult},
		{name: "missing at merge", compare: deliveryContainsJSON, merge: notFoundResult},
		{name: "directory", compare: deliveryContainsJSON, merge: ok(`[{"type":"file","path":"docs/specs/issue 7.md/a"}]`)},
		{name: "symlink", compare: deliveryContainsJSON, merge: ok(`{"type":"symlink","path":"docs/specs/issue 7.md","sha":"` + deliveryBlobSHA + `"}`)},
		{name: "merge not on base", compare: `{"status":"diverged","behind_by":2,"merge_base":"` + deliveryOtherSHA + `"}`},
		{name: "mismatched path metadata", compare: deliveryContainsJSON, merge: ok(`{"type":"file","path":"docs/other.md","sha":"` + deliveryBlobSHA + `"}`), wantErr: true},
		{name: "contents unavailable", compare: deliveryContainsJSON, merge: func() (shell.Result, error) {
			result := shell.Result{ExitCode: 1, Stderr: "gh: Server Error (HTTP 500)"}
			return result, &shell.CommandExecutionError{Message: "Command exited with code 1", Result: result}
		}, wantErr: true},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			routes := map[string]func() (shell.Result, error){
				deliveryBranchEndpoint:  ok(deliveryTipRef),
				deliveryCompareEndpoint: ok(tc.compare),
			}
			if tc.merge != nil {
				routes[deliveryContentsAtMerge] = tc.merge
			}
			if tc.tip != nil {
				routes[deliveryContentsAtTip] = tc.tip
			}
			gateway, _ := newDeliveryGateway(t, routes)
			got, err := gateway.DeliverySpecOnBase(context.Background(), "acme/looper", "main", deliverySpecPath, deliveryMergeSHA, "")
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("DeliverySpecOnBase() = %t, %v; want %t, err=%t", got, err, tc.want, tc.wantErr)
			}
		})
	}
}

func TestDeliverySpecOnBaseRejectsUnsafeInputWithoutCalls(t *testing.T) {
	t.Parallel()
	gateway, runner := newDeliveryGateway(t, nil)
	for _, path := range []string{"", "/etc/passwd", "../secret.md", "docs/../../x.md", "docs/./x.md", "docs//x.md", "docs/", `docs\x.md`, ".git/config", "docs/x\n.md"} {
		if _, err := gateway.DeliverySpecOnBase(context.Background(), "acme/looper", "main", path, deliveryMergeSHA, ""); err == nil {
			t.Fatalf("DeliverySpecOnBase(path=%q) error = nil", path)
		}
	}
	for _, base := range []string{"", "-main", "a..b", "main?ref=x", "refs/../main", "a b", "x.lock"} {
		if _, err := gateway.DeliverySpecOnBase(context.Background(), "acme/looper", base, "docs/x.md", deliveryMergeSHA, ""); err == nil {
			t.Fatalf("DeliverySpecOnBase(base=%q) error = nil", base)
		}
	}
	if _, err := gateway.DeliverySpecOnBase(context.Background(), "acme/looper", "main", "docs/x.md", "HEAD", ""); err == nil {
		t.Fatal("DeliverySpecOnBase(commit=HEAD) error = nil")
	}
	if len(runner.calls) != 0 {
		t.Fatalf("calls = %#v, want none", runner.calls)
	}
}

func TestDeliverySpecOnBaseRejectsAmbiguousBranchRef(t *testing.T) {
	t.Parallel()
	gateway, _ := newDeliveryGateway(t, map[string]func() (shell.Result, error){
		deliveryBranchEndpoint: ok(`{"ref":"refs/heads/main-2","object":{"type":"commit","sha":"` + deliveryTipSHA + `"}}`),
	})
	if _, err := gateway.DeliverySpecOnBase(context.Background(), "acme/looper", "main", "docs/x.md", deliveryMergeSHA, ""); err == nil {
		t.Fatal("DeliverySpecOnBase() error = nil, want ambiguous branch error")
	}
}

func deliveryFilesPage(entries ...string) string {
	return "[" + strings.Join(entries, ",") + "]"
}

func deliveryFileEntry(name, status string) string {
	return fmt.Sprintf(`{"filename":%q,"status":%q,"sha":%q}`, name, status, deliveryBlobSHA)
}

func deliveryFilesEndpoint(page int) string {
	return fmt.Sprintf("repos/acme/looper/pulls/7/files?per_page=100&page=%d", page)
}

func TestDeliverySpecInPullRequestStatuses(t *testing.T) {
	t.Parallel()
	for status, want := range map[string]bool{"added": true, "modified": true, "renamed": true, "removed": false, "copied": false, "unchanged": false} {
		status, want := status, want
		t.Run(status, func(t *testing.T) {
			t.Parallel()
			gateway, _ := newDeliveryGateway(t, map[string]func() (shell.Result, error){
				deliveryPREndpoint:       ok(deliveryPRJSON("open", false, "", "acme/looper", 2)),
				deliveryFilesEndpoint(1): ok(deliveryFilesPage(deliveryFileEntry("main.go", "modified"), deliveryFileEntry("docs/spec.md", status))),
			})
			got, err := gateway.DeliverySpecInPullRequest(context.Background(), "acme/looper", 7, "docs/spec.md", "")
			if err != nil || got != want {
				t.Fatalf("DeliverySpecInPullRequest() = %t, %v; want %t", got, err, want)
			}
		})
	}
}

func TestDeliverySpecInPullRequestPagesAndAbsentPath(t *testing.T) {
	t.Parallel()
	page1 := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		page1 = append(page1, deliveryFileEntry(fmt.Sprintf("src/f%d.go", i), "modified"))
	}
	for name, tc := range map[string]struct {
		lastEntry string
		want      bool
	}{
		"found on second page": {lastEntry: deliveryFileEntry("docs/spec.md", "added"), want: true},
		"absent":               {lastEntry: deliveryFileEntry("docs/other.md", "added")},
		"prefix only":          {lastEntry: deliveryFileEntry("docs/spec.md.bak", "added")},
	} {
		tc := tc
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gateway, runner := newDeliveryGateway(t, map[string]func() (shell.Result, error){
				deliveryPREndpoint:       ok(deliveryPRJSON("closed", true, deliveryMergeSHA, "acme/looper", 101)),
				deliveryFilesEndpoint(1): ok(deliveryFilesPage(page1...)),
				deliveryFilesEndpoint(2): ok(deliveryFilesPage(tc.lastEntry)),
			})
			got, err := gateway.DeliverySpecInPullRequest(context.Background(), "acme/looper", 7, "docs/spec.md", "")
			if err != nil || got != tc.want {
				t.Fatalf("DeliverySpecInPullRequest() = %t, %v; want %t", got, err, tc.want)
			}
			if len(runner.calls) != 3 {
				t.Fatalf("calls = %#v, want PR + 2 pages", runner.calls)
			}
		})
	}
}

func TestDeliverySpecInPullRequestRefusesIncompleteEvidence(t *testing.T) {
	t.Parallel()
	cases := map[string]map[string]func() (shell.Result, error){
		"over github file limit": {
			deliveryPREndpoint: ok(deliveryPRJSON("open", false, "", "acme/looper", 3001)),
		},
		"short list": {
			deliveryPREndpoint:       ok(deliveryPRJSON("open", false, "", "acme/looper", 3)),
			deliveryFilesEndpoint(1): ok(deliveryFilesPage(deliveryFileEntry("docs/spec.md", "added"))),
		},
		"missing count": {
			deliveryPREndpoint: ok(strings.Replace(deliveryPRJSON("open", false, "", "acme/looper", 1), `"changed_files":1,`, "", 1)),
		},
		"duplicate entries": {
			deliveryPREndpoint:       ok(deliveryPRJSON("open", false, "", "acme/looper", 2)),
			deliveryFilesEndpoint(1): ok(deliveryFilesPage(deliveryFileEntry("docs/spec.md", "added"), deliveryFileEntry("docs/spec.md", "modified"))),
		},
		"truncated output": {
			deliveryPREndpoint: ok(deliveryPRJSON("open", false, "", "acme/looper", 1)),
			deliveryFilesEndpoint(1): func() (shell.Result, error) {
				return shell.Result{Stdout: `[{"filename":"docs/spec.md"`, StdoutTruncated: true}, nil
			},
		},
		"fork pull request": {
			deliveryPREndpoint: ok(deliveryPRJSON("open", false, "", "mallory/looper", 1)),
		},
		"unknown pull request": {deliveryPREndpoint: notFoundResult},
	}
	for name, routes := range cases {
		routes := routes
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			gateway, _ := newDeliveryGateway(t, routes)
			if got, err := gateway.DeliverySpecInPullRequest(context.Background(), "acme/looper", 7, "docs/spec.md", ""); err == nil {
				t.Fatalf("DeliverySpecInPullRequest() = %t, want error", got)
			}
		})
	}
}

func TestDeliverySpecInPullRequestRejectsTraversalWithoutCalls(t *testing.T) {
	t.Parallel()
	gateway, runner := newDeliveryGateway(t, nil)
	for _, path := range []string{"../spec.md", "/docs/spec.md", "docs/../spec.md"} {
		if _, err := gateway.DeliverySpecInPullRequest(context.Background(), "acme/looper", 7, path, ""); err == nil {
			t.Fatalf("DeliverySpecInPullRequest(%q) error = nil", path)
		}
	}
	if len(runner.calls) != 0 {
		t.Fatalf("calls = %#v, want none", runner.calls)
	}
}

func TestCloseDeliveredIssueClosesAsCompleted(t *testing.T) {
	t.Parallel()
	runner := &fakeGHRunner{t: t}
	state := "open"
	runner.respond = func(options shell.Options) (shell.Result, error) {
		args := strings.Join(options.Args, " ")
		switch args {
		case "api repos/acme/looper/issues/9 --jq .state":
			return shell.Result{Stdout: state + "\n"}, nil
		case "issue close 9 --repo acme/looper --reason completed":
			state = "closed"
			return shell.Result{}, nil
		}
		t.Fatalf("unexpected gh args: %q", args)
		return shell.Result{}, nil
	}
	gateway := New(Options{GHPath: "gh", GHRun: runner.run})
	if err := gateway.CloseDeliveredIssue(context.Background(), "acme/looper", 9, ""); err != nil {
		t.Fatalf("CloseDeliveredIssue() error = %v", err)
	}
	if err := gateway.CloseDeliveredIssue(context.Background(), "acme/looper", 9, ""); err != nil {
		t.Fatalf("second CloseDeliveredIssue() error = %v", err)
	}
	closes := 0
	for _, call := range runner.calls {
		if strings.HasPrefix(call, "issue close") {
			closes++
		}
	}
	if closes != 1 {
		t.Fatalf("calls = %#v, want exactly one close", runner.calls)
	}
	if err := gateway.CloseDeliveredIssue(context.Background(), "acme/looper", 0, ""); err == nil {
		t.Fatal("CloseDeliveredIssue(issue=0) error = nil")
	}
}
