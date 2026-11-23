package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/nexu-io/looper/internal/agent"
	"github.com/nexu-io/looper/internal/delivery"
	"github.com/nexu-io/looper/internal/infra/specpr"
	"github.com/nexu-io/looper/internal/storage"
)

func (c *workerCheckpoint) rememberInspection(i InspectHeadResult) {
	if e := c.Execution; e != nil {
		e.LocalHead = i.HeadSHA
		e.LocalCommits = i.NewCommitSHAs
		e.CommittedFiles = i.CommittedFiles
	}
}

func checkPendingBinding(c workerCheckpoint, l storage.LoopRecord) error {
	d, w := c.Delivery, c.Work
	if d == nil || w == nil || c.Execution == nil || validateCompletedExecutionCheckpoint(c.Execution) != nil || c.PullRequest == nil || c.SkipReason != "" || d.WorkerLoopID != l.ID || d.ProjectID != l.ProjectID || d.PRNumber <= 0 || d.PRNumber != c.PullRequest.Number || d.IssueNumber <= 0 || w.IssueNumber != d.IssueNumber || w.Repo != d.Repo || w.BaseBranch != d.BaseBranch || d.HeadSHA == "" || d.HeadSHA != c.Execution.LocalHead || d.RunID == "" || (l.PRNumber != nil && *l.PRNumber != d.PRNumber) {
		return &loopError{message: "pending implementation delivery has inconsistent bindings; manual recovery required", kind: FailureManualIntervention}
	}
	return nil
}

func prBoundTo(p PullRequestDetail, repo, base string, pr int64) bool {
	u, err := url.Parse(p.URL)
	return err == nil && u.Host != "" && (strings.EqualFold(u.Path, fmt.Sprintf("/%s/pull/%d", repo, pr)) || strings.EqualFold(u.Path, fmt.Sprintf("/%s/pulls/%d", repo, pr))) && p.Number == pr && p.BaseRefName == base
}

func (r *Runner) preflightExistingPR(ctx context.Context, w workerInput, pr int64, cwd string) (PullRequestDetail, error) {
	p, err := r.github.ViewPullRequest(ctx, ViewPullRequestInput{Repo: w.Repo, PRNumber: pr, CWD: cwd})
	if err != nil {
		return p, err
	}
	if !prBoundTo(p, w.Repo, w.BaseBranch, pr) || closingKeyword.MatchString(p.Body) {
		return p, &loopError{message: fmt.Sprintf("PR #%d has unexpected bindings or an existing closing reference; review manually before pushing", pr), kind: FailureManualIntervention}
	}
	return p, nil
}

func overlappingImplementation(e *checkpointExecution, v *ValidationResult, spec string) bool {
	if e == nil || v == nil {
		return false
	}
	for _, f := range v.PRFiles {
		if containsString(e.CommittedFiles, f) && implementationFiles([]string{f}, spec) {
			return true
		}
	}
	return false
}

func declaredFull(e *checkpointExecution, issue int64) bool {
	if e == nil || e.ParseStatus != "parsed" || e.Status != "completed" || issue <= 0 {
		return false
	}
	// Require a unique structured declaration, never infer scope from prose.
	var found bool
	for _, line := range strings.Split(e.Stdout, "\n") {
		raw, ok := strings.CutPrefix(strings.TrimSpace(line), agent.CompletionMarkerPrefix)
		if !ok {
			continue
		}
		var claim struct {
			IssueNumber     int64
			IssueResolution string
			Summary         string
		}
		if found || json.Unmarshal([]byte(raw), &claim) != nil || claim.IssueNumber != issue || claim.IssueResolution != "full" || claim.Summary != e.Summary {
			return false
		}
		found = true
	}
	return found
}

func implementationFiles(files []string, spec string) bool {
	for _, f := range files {
		p := strings.ToLower(f)
		if f == spec || strings.HasPrefix(p, "docs/") || strings.HasPrefix(p, "specs/") {
			continue
		}
		switch path.Ext(p) {
		case ".go", ".py", ".ts", ".js", ".tsx", ".jsx", ".rs", ".c", ".cpp", ".java", ".rb", ".swift", ".sh", ".sql":
			return true
		}
	}
	return false
}

const publicationStart = "<!-- looper:implementation:start -->"
const publicationEnd = "<!-- looper:implementation:end -->"

func (r *Runner) normalizeWorkerDisclosure(ctx context.Context, run storage.RunRecord, work workerInput, pr int64, cwd string, force bool) error {
	if work.IssueNumber > 0 {
		return nil
	} // Issue publication applies disclosure with the verified body write.
	return r.normalizePullRequestDisclosure(ctx, run, work.Repo, pr, cwd, force)
}

// A model summary or an echoed issue description is not authority to close issues.
var closingKeyword = regexp.MustCompile(`(?i)\b(?:close[sd]?|fix(?:es|ed)?|resolve[sd]?)(:?\s+)((?:[\w.-]+/[\w.-]+)?#[1-9]\d*|https?://[^\s/]+/[\w.-]+/[\w.-]+/issues/[1-9]\d*)\b`)

func publicationBody(body string, w workerInput, d delivery.Record, e *checkpointExecution, plannerOwned bool) (string, error) {
	block := fmt.Sprintf("%s\n## Implementation\nRefs %s\n\nPublished head: `%s`", publicationStart, formatIssueClosingReference(w.Repo, w.IssueRepo, w.IssueNumber), d.HeadSHA)
	if e != nil {
		summary := strings.Join(strings.Fields(e.Summary), " ")
		if !strings.Contains(summary, "<!--") && summary != "" {
			block += "\n\n" + closingKeyword.ReplaceAllString(summary, "Refs${1}${2}")
		}
	}
	if d.SpecPath != "" {
		block += "\nSpec: " + d.SpecPath
	}
	if d.ValidationRef != "" {
		block += "\nValidation: `" + d.ValidationRef + "`"
	}
	if len(d.Commits) > 0 {
		block += "\nImplementation commits: `" + strings.Join(d.Commits, "`, `") + "`"
	}
	block += "\n" + publicationEnd
	if strings.Contains(body, publicationStart) || strings.Contains(body, publicationEnd) {
		a, b := strings.Index(body, publicationStart), strings.Index(body, publicationEnd)
		if a < 0 || b < a || strings.Count(body, publicationStart) != 1 || strings.Count(body, publicationEnd) != 1 {
			return "", fmt.Errorf("ambiguous implementation markers")
		}
		return body[:a] + block + body[b+len(publicationEnd):], nil
	}
	legacy := fmt.Sprintf("## Summary\n- Adds the planning spec for %s#%d\n- Spec path: %s\n- Planner branch: ", issueLookupRepo(w), w.IssueNumber, w.SpecPath)
	if plannerOwned && strings.Contains(body, specpr.ManagedPlanStart+"\n"+legacy) {
		a, b := strings.Index(body, specpr.ManagedPlanStart), strings.Index(body, specpr.ManagedPlanEnd)
		if strings.Count(body, specpr.ManagedPlanStart) != 1 || strings.Count(body, specpr.ManagedPlanEnd) != 1 || b < a {
			return "", fmt.Errorf("ambiguous planner markers")
		}
		return body[:a] + block + body[b+len(specpr.ManagedPlanEnd):], nil
	}
	// Unmarked legacy summaries may have been edited by a human. Keep them intact.
	if plannerOwned && strings.HasPrefix(body, legacy) {
		return block + "\n\n### Original planning context\n\n" + body, nil
	}
	if body == "" {
		return block, nil
	}
	return body + "\n\n" + block, nil
}

func (r *Runner) persistDelivery(ctx context.Context, id string, d delivery.Record) error {
	return storage.WithTransaction(ctx, r.db, nil, func(tx *sql.Tx) error {
		repo := storage.NewRepositories(tx).Loops
		loop, err := repo.GetByID(ctx, id)
		if err != nil {
			return err
		}
		if loop == nil {
			return fmt.Errorf("worker loop %s missing", id)
		}
		if loop.MetadataJSON != nil && strings.TrimSpace(*loop.MetadataJSON) != "" && !json.Valid([]byte(*loop.MetadataJSON)) {
			return fmt.Errorf("invalid worker loop metadata")
		}
		raw, err := mergeLoopMetadataJSON(loop.MetadataJSON, map[string]any{"implementationDelivery": d})
		if err != nil {
			return err
		}
		loop.MetadataJSON = &raw
		loop.UpdatedAt = r.nowISO()
		return repo.Upsert(ctx, *loop)
	})
}

func (r *Runner) finishPublication(ctx context.Context, in stepInput, c workerCheckpoint) (workerCheckpoint, error) {
	w := *c.Work
	if w.IssueNumber <= 0 {
		return c, nil
	}
	if c.Delivery == nil {
		e := c.Execution
		d := delivery.Record{ProjectID: in.Project.ID, Repo: w.Repo, WorkerLoopID: in.Loop.ID, RunID: in.Run.ID, IssueNumber: w.IssueNumber, PRNumber: c.PullRequest.Number, BaseBranch: w.BaseBranch, SpecPath: w.SpecPath, MetadataPending: true}
		if e != nil {
			d.HeadSHA = e.LocalHead
			d.Commits = e.LocalCommits
		}
		if v := c.Validation; v != nil && v.RunID != "" && v.Passed && len(v.Commands) > 0 && v.ValidatedHead != "" && v.ValidatedHead == d.HeadSHA {
			d.ValidationRef = v.RunID + ":validate"
		}
		c.Delivery = &d
	}
	d := *c.Delivery
	fail := func(err error) (workerCheckpoint, error) {
		return c, &loopError{message: "worker publication: " + err.Error(), kind: FailureRetryableAfterResume}
	}
	if d.ProjectID != in.Project.ID || d.WorkerLoopID != in.Loop.ID || d.Repo != w.Repo || d.IssueNumber != w.IssueNumber || c.PullRequest == nil || c.PullRequest.Number != d.PRNumber || w.BaseBranch != d.BaseBranch {
		return fail(fmt.Errorf("checkpoint binding mismatch"))
	}
	view := ViewPullRequestInput{Repo: w.Repo, PRNumber: d.PRNumber, CWD: in.Project.RepoPath}
	read := func() (PullRequestDetail, error) {
		p, err := r.github.ViewPullRequest(ctx, view)
		if err != nil {
			return p, err
		}
		if !prBoundTo(p, d.Repo, d.BaseBranch, d.PRNumber) || p.HeadSHA == "" || !strings.EqualFold(p.HeadSHA, d.HeadSHA) {
			return p, fmt.Errorf("PR #%d does not match published repository, base or head", d.PRNumber)
		}
		return p, nil
	}
	p, err := read()
	if err != nil {
		if in.Checkpoint.Delivery == nil {
			c.Delivery = nil
		}
		return fail(err)
	}
	if err := r.persistCheckpoint(ctx, in.Run.ID, c); err != nil {
		return fail(err)
	}
	if err := r.persistDelivery(ctx, in.Loop.ID, d); err != nil {
		return fail(err)
	}
	// Closing keywords can act on merge, before the coordinator checks the merged head.
	// The existing text may be human-authored: never silently edit it or publish a
	// contradictory Refs block. Keep the metadata checkpoint until a person resolves it.
	if closingKeyword.MatchString(p.Body) {
		return c, &loopError{message: fmt.Sprintf("PR #%d already contains an issue-closing reference; review the body manually before publishing", d.PRNumber), kind: FailureManualIntervention}
	}
	records, err := r.repos.Loops.ListFiltered(ctx, storage.ListLoopsOptions{ProjectID: d.ProjectID})
	if err != nil {
		return fail(err)
	}
	plannerOwned := false
	for _, l := range records {
		if l.Type != "planner" || l.TargetType != "issue" || !strings.EqualFold(derefString(l.TargetID), buildIssueTargetID(d.Repo, d.IssueNumber)) {
			continue
		}
		m := parseJSONObject(l.MetadataJSON)
		if w.ExecutionMode == "push-existing" && d.SpecPath != "" && stringFromAnyDefault(m["specPath"]) == d.SpecPath && firstNonZero(derefInt64(l.PRNumber), int64FromAny(m["prNumber"])) == d.PRNumber {
			plannerOwned = true
			break
		}
	}
	body, err := publicationBody(p.Body, w, d, c.Execution, plannerOwned)
	if err != nil {
		return fail(err)
	}
	body = r.stampPullRequestDisclosure(in.Run, body)
	if p.Body != body {
		agent, model := r.disclosureIdentity(in.Run)
		if err := r.github.UpdatePullRequestBody(ctx, UpdatePullRequestBodyInput{Repo: w.Repo, PRNumber: d.PRNumber, CWD: in.Project.RepoPath, Body: body, DisclosureAgent: agent, DisclosureModel: model}); err != nil {
			return fail(err)
		}
		p, err = read()
		if err != nil {
			return fail(err)
		}
		if p.Body != body {
			return fail(fmt.Errorf("PR body read-back mismatch"))
		}
	}
	d.Published = true
	d.MetadataPending = false
	d.Complete = d.VerifiedFor(in.Project.ID, w.Repo, w.IssueNumber, d.PRNumber) && strings.EqualFold(issueLookupRepo(w), w.Repo) && d.ValidationRef != "" && declaredFull(c.Execution, w.IssueNumber) && overlappingImplementation(c.Execution, c.Validation, w.SpecPath)
	if d.Complete {
		d.Complete = uniquePublicationIssue(records, d)
	}
	if !strings.EqualFold(issueLookupRepo(w), w.Repo) {
		d.Published = false
	} // Record has no cross-repository issue identity.
	if err := r.persistDelivery(ctx, in.Loop.ID, d); err != nil {
		return fail(err)
	}
	c.Delivery = &d
	return c, nil
}

// A second durable issue association for this PR makes a single-issue declaration ambiguous.
func uniquePublicationIssue(records []storage.LoopRecord, d delivery.Record) bool {
	for _, l := range records {
		if l.ProjectID != d.ProjectID || (l.Type != "worker" && l.Type != "planner") {
			continue
		}
		m := parseJSONObject(l.MetadataJSON)
		worker, _ := m["worker"].(map[string]any)
		repo := firstNonEmpty(derefString(l.Repo), stringFromAnyDefault(m["repo"]), stringFromAnyDefault(worker["repo"]))
		pr := firstNonZero(derefInt64(l.PRNumber), int64FromAny(m["prNumber"]), int64FromAny(worker["prNumber"]))
		if pr != d.PRNumber || !strings.EqualFold(repo, d.Repo) {
			continue
		}
		if l.TargetType == "issue" && !strings.EqualFold(derefString(l.TargetID), buildIssueTargetID(d.Repo, d.IssueNumber)) {
			return false
		}
		if issue := int64FromAny(worker["issueNumber"]); issue != 0 && (issue != d.IssueNumber || !strings.EqualFold(firstNonEmpty(stringFromAnyDefault(worker["issueRepo"]), repo), d.Repo)) {
			return false
		}
	}
	return true
}
