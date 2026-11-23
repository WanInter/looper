// Package delivery defines evidence shared by publication and merge reconciliation.
package delivery

import (
	"encoding/hex"
	"encoding/json"
	"strings"
)

type Record struct {
	ProjectID       string   `json:"projectId"`
	Repo            string   `json:"repo"`
	WorkerLoopID    string   `json:"workerLoopId"`
	RunID           string   `json:"runId"`
	IssueNumber     int64    `json:"issueNumber"`
	PRNumber        int64    `json:"prNumber"`
	BaseBranch      string   `json:"baseBranch"`
	HeadSHA         string   `json:"headSha"`
	SpecPath        string   `json:"specPath,omitempty"`
	Commits         []string `json:"commits"`
	ValidationRef   string   `json:"validationRef,omitempty"`
	Published       bool     `json:"published"`
	MetadataPending bool     `json:"metadataPending"`
	Complete        bool     `json:"complete"`
}

func FromMetadata(raw *string) (*Record, error) {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return nil, nil
	}
	var m struct {
		Record *Record `json:"implementationDelivery"`
	}
	err := json.Unmarshal([]byte(*raw), &m)
	return m.Record, err
}

func validOID(s string) bool {
	if len(s) != 40 && len(s) != 64 || strings.Trim(s, "0") == "" {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func (r Record) VerifiedFor(projectID, repo string, issue, pr int64) bool {
	if !r.Published || r.MetadataPending || projectID == "" || repo == "" || issue <= 0 || pr <= 0 || r.ProjectID != projectID || !strings.EqualFold(r.Repo, repo) || r.IssueNumber != issue || r.PRNumber != pr || r.WorkerLoopID == "" || r.RunID == "" || r.BaseBranch == "" || !validOID(r.HeadSHA) {
		return false
	}
	head := false
	for _, s := range r.Commits {
		if !validOID(s) {
			return false
		}
		head = head || strings.EqualFold(s, r.HeadSHA)
	}
	return head
}
