package github

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/nexu-io/looper/internal/delivery"
)

// GitHub returns at most 3000 files for a pull request; larger file lists are
// silently truncated, so a PR with more changed files cannot prove membership.
const (
	deliveryPullRequestFilesPerPage = 100
	deliveryPullRequestFilesMax     = 3000
)

var (
	deliveryRepoPathPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	deliveryCommitSHA       = regexp.MustCompile(`^[0-9a-f]{40}$`)
)

// DeliveryPullRequest reads the pull request through the REST API and verifies
// that both its base and head live in repo, so evidence from a fork or from a
// PR number bound to another repository is rejected. A missing PR is returned
// as an error (unknown), never as CLOSED. For merged PRs the canonical
// merge_commit_sha (merge, squash, or rebase result) is compared with the
// current base branch tip; the original head commit is never required to be
// an ancestor of the base.
func (g *Gateway) DeliveryPullRequest(ctx context.Context, repo string, pr int64, cwd string) (delivery.PullRequest, error) {
	hostname, repoPath, err := parseDeliveryRepo(repo)
	if err != nil {
		return delivery.PullRequest{}, err
	}
	row, err := g.deliveryPullRequestRow(ctx, hostname, repoPath, pr, cwd)
	if err != nil {
		return delivery.PullRequest{}, err
	}
	out := delivery.PullRequest{
		Number:     pr,
		Repo:       strings.TrimSpace(repo),
		BaseBranch: nestedString(row, "base", "ref"),
		HeadBranch: nestedString(row, "head", "ref"),
		HeadSHA:    nestedString(row, "head", "sha"),
	}
	state := strings.ToLower(asString(row["state"]))
	merged, known := row["merged"].(bool)
	if !known {
		return delivery.PullRequest{}, fmt.Errorf("pull request %s#%d has no verified merge status", repoPath, pr)
	}
	switch {
	case merged && state == "closed":
		out.State = delivery.PullRequestMerged
	case merged:
		return delivery.PullRequest{}, fmt.Errorf("pull request %s#%d reports merged with state %q", repoPath, pr, state)
	case state == "open":
		out.State = delivery.PullRequestOpen
		return out, nil
	case state == "closed":
		out.State = delivery.PullRequestClosed
		return out, nil
	default:
		return delivery.PullRequest{}, fmt.Errorf("pull request %s#%d has unknown state %q", repoPath, pr, state)
	}
	out.MergeSHA = asString(row["merge_commit_sha"])
	if !deliveryCommitSHA.MatchString(out.MergeSHA) {
		return delivery.PullRequest{}, fmt.Errorf("merged pull request %s#%d has invalid merge commit %q", repoPath, pr, out.MergeSHA)
	}
	tip, err := g.deliveryBranchTip(ctx, hostname, repoPath, out.BaseBranch, cwd)
	if err != nil {
		return delivery.PullRequest{}, err
	}
	out.BaseContainsMerge, err = g.deliveryCommitContains(ctx, hostname, repoPath, tip, out.MergeSHA, cwd)
	if err != nil {
		return delivery.PullRequest{}, err
	}
	return out, nil
}

// DeliverySpecOnBase reports whether the current tip of base contains
// mergedCommit and both commits hold path as the same regular file blob.
// Missing files, directories, symlinks, submodules, or differing blobs are a
// definite false; unreadable or malformed evidence is an error.
func (g *Gateway) DeliverySpecOnBase(ctx context.Context, repo, base, path, mergedCommit, cwd string) (bool, error) {
	hostname, repoPath, err := parseDeliveryRepo(repo)
	if err != nil {
		return false, err
	}
	if err := validateDeliverySpecPath(path); err != nil {
		return false, err
	}
	if !deliveryCommitSHA.MatchString(mergedCommit) {
		return false, fmt.Errorf("invalid merged commit %q", mergedCommit)
	}
	tip, err := g.deliveryBranchTip(ctx, hostname, repoPath, base, cwd)
	if err != nil {
		return false, err
	}
	contains, err := g.deliveryCommitContains(ctx, hostname, repoPath, tip, mergedCommit, cwd)
	if err != nil || !contains {
		return false, err
	}
	mergedBlob, err := g.deliveryFileBlob(ctx, hostname, repoPath, path, mergedCommit, cwd)
	if err != nil || mergedBlob == "" {
		return false, err
	}
	tipBlob, err := g.deliveryFileBlob(ctx, hostname, repoPath, path, tip, cwd)
	if err != nil || tipBlob == "" {
		return false, err
	}
	return mergedBlob == tipBlob, nil
}

// DeliverySpecInPullRequest reports whether the PR's file list adds, modifies,
// or renames a file to exactly path. Deletions do not count. The file list is
// paged up to GitHub's 3000-file limit and must account for every changed file
// the PR reports; a larger, truncated, or shifting list is an error.
func (g *Gateway) DeliverySpecInPullRequest(ctx context.Context, repo string, pr int64, path, cwd string) (bool, error) {
	hostname, repoPath, err := parseDeliveryRepo(repo)
	if err != nil {
		return false, err
	}
	if err := validateDeliverySpecPath(path); err != nil {
		return false, err
	}
	row, err := g.deliveryPullRequestRow(ctx, hostname, repoPath, pr, cwd)
	if err != nil {
		return false, err
	}
	changed, ok := row["changed_files"].(float64)
	if !ok || changed < 0 || changed != float64(int64(changed)) {
		return false, fmt.Errorf("pull request %s#%d has no usable changed_files count", repoPath, pr)
	}
	if changed > deliveryPullRequestFilesMax {
		return false, fmt.Errorf("pull request %s#%d changes %d files; file list beyond %d is truncated", repoPath, pr, int64(changed), deliveryPullRequestFilesMax)
	}
	pages := (int(changed) + deliveryPullRequestFilesPerPage - 1) / deliveryPullRequestFilesPerPage
	seen, matches := 0, 0
	for page := 1; page <= pages; page++ {
		endpoint := fmt.Sprintf("repos/%s/pulls/%d/files?per_page=%d&page=%d", repoPath, pr, deliveryPullRequestFilesPerPage, page)
		result, err := g.runGh(ctx, cwd, "", deliveryAPIArgs(hostname, endpoint)...)
		if err != nil {
			return false, err
		}
		files, err := decodeJSONArray(result.Stdout)
		if err != nil {
			return false, err
		}
		seen += len(files)
		for _, file := range files {
			if asString(file["filename"]) != path {
				continue
			}
			switch asString(file["status"]) {
			case "added", "modified", "renamed":
				matches++
			}
		}
	}
	if seen != int(changed) {
		return false, fmt.Errorf("pull request %s#%d file list has %d entries, want %d", repoPath, pr, seen, int64(changed))
	}
	if matches > 1 {
		return false, fmt.Errorf("pull request %s#%d lists %s %d times", repoPath, pr, path, matches)
	}
	return matches == 1, nil
}

// CloseDeliveredIssue closes the issue as completed; an already closed issue
// is left untouched, matching CloseIssue.
func (g *Gateway) CloseDeliveredIssue(ctx context.Context, repo string, issue int64, cwd string) error {
	if _, _, err := parseDeliveryRepo(repo); err != nil {
		return err
	}
	if issue <= 0 {
		return fmt.Errorf("invalid issue number %d", issue)
	}
	return g.CloseIssue(ctx, CloseIssueInput{Repo: strings.TrimSpace(repo), IssueNumber: issue, StateReason: "completed", CWD: cwd})
}

func (g *Gateway) deliveryPullRequestRow(ctx context.Context, hostname, repoPath string, pr int64, cwd string) (map[string]any, error) {
	if pr <= 0 {
		return nil, fmt.Errorf("invalid pull request number %d", pr)
	}
	result, err := g.runGh(ctx, cwd, "", deliveryAPIArgs(hostname, fmt.Sprintf("repos/%s/pulls/%d", repoPath, pr))...)
	if err != nil {
		return nil, fmt.Errorf("read pull request %s#%d: %w", repoPath, pr, err)
	}
	row, err := decodeJSONObject(result.Stdout)
	if err != nil {
		return nil, err
	}
	if got := asInt64(row["number"]); got != pr {
		return nil, fmt.Errorf("pull request %s#%d response is for #%d", repoPath, pr, got)
	}
	baseRepo := nestedString(row, "base", "repo", "full_name")
	if !strings.EqualFold(baseRepo, repoPath) {
		return nil, fmt.Errorf("pull request %s#%d base repository is %q", repoPath, pr, baseRepo)
	}
	headRepo := nestedString(row, "head", "repo", "full_name")
	if !strings.EqualFold(headRepo, repoPath) {
		return nil, fmt.Errorf("pull request %s#%d head repository is %q, want %s", repoPath, pr, headRepo, repoPath)
	}
	for _, branch := range []string{nestedString(row, "base", "ref"), nestedString(row, "head", "ref")} {
		if err := validateDeliveryBranch(branch); err != nil {
			return nil, fmt.Errorf("pull request %s#%d: %w", repoPath, pr, err)
		}
	}
	if sha := nestedString(row, "head", "sha"); !deliveryCommitSHA.MatchString(sha) {
		return nil, fmt.Errorf("pull request %s#%d has invalid head commit %q", repoPath, pr, sha)
	}
	return row, nil
}

func (g *Gateway) deliveryBranchTip(ctx context.Context, hostname, repoPath, branch, cwd string) (string, error) {
	if err := validateDeliveryBranch(branch); err != nil {
		return "", err
	}
	endpoint := fmt.Sprintf("repos/%s/git/ref/heads/%s", repoPath, escapeDeliveryPath(branch))
	result, err := g.runGh(ctx, cwd, "", deliveryAPIArgs(hostname, endpoint)...)
	if err != nil {
		return "", fmt.Errorf("read branch %s in %s: %w", branch, repoPath, err)
	}
	row, err := decodeJSONObject(result.Stdout)
	if err != nil {
		return "", err
	}
	sha := nestedString(row, "object", "sha")
	if asString(row["ref"]) != "refs/heads/"+branch || nestedString(row, "object", "type") != "commit" || !deliveryCommitSHA.MatchString(sha) {
		return "", fmt.Errorf("branch %s in %s did not resolve to exactly one commit", branch, repoPath)
	}
	return sha, nil
}

// deliveryCommitContains reports whether commit is reachable from tip.
func (g *Gateway) deliveryCommitContains(ctx context.Context, hostname, repoPath, tip, commit, cwd string) (bool, error) {
	if tip == commit {
		return true, nil
	}
	endpoint := fmt.Sprintf("repos/%s/compare/%s...%s", repoPath, commit, tip)
	args := append(deliveryAPIArgs(hostname, endpoint), "--jq", "{status: .status, behind_by: .behind_by, merge_base: .merge_base_commit.sha}")
	result, err := g.runGh(ctx, cwd, "", args...)
	if err != nil {
		return false, fmt.Errorf("compare %s with %s in %s: %w", commit, tip, repoPath, err)
	}
	row, err := decodeJSONObject(result.Stdout)
	if err != nil {
		return false, err
	}
	status := asString(row["status"])
	behind, hasBehind := row["behind_by"].(float64)
	switch {
	case (status == "ahead" || status == "identical") && hasBehind && behind == 0 && asString(row["merge_base"]) == commit:
		return true, nil
	case (status == "behind" || status == "diverged") && hasBehind && behind > 0:
		return false, nil
	default:
		return false, fmt.Errorf("compare %s with %s in %s returned inconsistent status %q", commit, tip, repoPath, status)
	}
}

// deliveryFileBlob returns the blob SHA of path at ref, or "" when path is
// absent or not a regular file there.
func (g *Gateway) deliveryFileBlob(ctx context.Context, hostname, repoPath, path, ref, cwd string) (string, error) {
	endpoint := fmt.Sprintf("repos/%s/contents/%s?ref=%s", repoPath, escapeDeliveryPath(path), url.QueryEscape(ref))
	result, err := g.runGh(ctx, cwd, "", deliveryAPIArgs(hostname, endpoint)...)
	if err != nil {
		if IsNotFoundError(err) {
			return "", nil
		}
		return "", fmt.Errorf("read %s at %s in %s: %w", path, ref, repoPath, err)
	}
	if strings.HasPrefix(strings.TrimSpace(result.Stdout), "[") {
		return "", nil
	}
	row, err := decodeJSONObject(result.Stdout)
	if err != nil {
		return "", err
	}
	if asString(row["type"]) != "file" {
		return "", nil
	}
	sha := asString(row["sha"])
	if asString(row["path"]) != path || !deliveryCommitSHA.MatchString(sha) {
		return "", fmt.Errorf("read %s at %s in %s returned mismatched file metadata", path, ref, repoPath)
	}
	return sha, nil
}

func deliveryAPIArgs(hostname, endpoint string) []string {
	args := []string{"api", endpoint, "-H", "Accept: application/vnd.github+json"}
	if hostname != "" {
		args = append(args, "--hostname", hostname)
	}
	return args
}

func parseDeliveryRepo(repo string) (string, string, error) {
	hostname, repoPath := splitRepoHostname(repo)
	owner, name := splitRepoOwnerName(repoPath)
	if !deliveryRepoPathPattern.MatchString(repoPath) || owner == "." || owner == ".." || name == "." || name == ".." {
		return "", "", fmt.Errorf("invalid repository %q", repo)
	}
	return hostname, repoPath, nil
}

// validateDeliverySpecPath accepts only canonical repository-relative paths.
func validateDeliverySpecPath(path string) error {
	if path == "" || strings.HasPrefix(path, "/") || strings.HasSuffix(path, "/") || strings.Contains(path, `\`) {
		return fmt.Errorf("invalid spec path %q", path)
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "" || segment == "." || segment == ".." || segment == ".git" {
			return fmt.Errorf("invalid spec path %q", path)
		}
	}
	for _, r := range path {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("invalid spec path %q", path)
		}
	}
	return nil
}

// validateDeliveryBranch rejects names git itself refuses (git check-ref-format)
// so they cannot change the meaning of an API path.
func validateDeliveryBranch(branch string) error {
	invalid := branch == "" || branch == "@" || strings.HasPrefix(branch, "-") ||
		strings.HasPrefix(branch, "/") || strings.HasSuffix(branch, "/") || strings.HasSuffix(branch, ".") ||
		strings.Contains(branch, "..") || strings.Contains(branch, "//") || strings.Contains(branch, "@{") ||
		strings.ContainsAny(branch, " ~^:?*[\\")
	for _, segment := range strings.Split(branch, "/") {
		invalid = invalid || strings.HasPrefix(segment, ".") || strings.HasSuffix(segment, ".lock")
	}
	for _, r := range branch {
		invalid = invalid || r < 0x20 || r == 0x7f
	}
	if invalid {
		return fmt.Errorf("invalid branch name %q", branch)
	}
	return nil
}

func escapeDeliveryPath(path string) string {
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		segments[i] = url.PathEscape(segment)
	}
	return strings.Join(segments, "/")
}
