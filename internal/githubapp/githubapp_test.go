package githubapp_test

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/Lakr233/mini-control-action/internal/config"
	"github.com/Lakr233/mini-control-action/internal/githubapp"
	"github.com/Lakr233/mini-control-action/internal/githubapp/ghfake"
)

func newClient(t *testing.T) (*githubapp.Client, *ghfake.Server) {
	t.Helper()
	srv := ghfake.New()
	t.Cleanup(srv.Close)
	c, err := githubapp.New(config.GitHub{
		APIBaseURL: srv.URL, Scope: "repo", Owner: "acme", Repo: "widgets",
		RunnerGroupID: 1, Auth: config.GitHubAuth{Token: "ghp_test"},
	}, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	c.SetReleasesBaseForTest(srv.URL)
	return c, srv
}

func TestJITAndRunnerLifecycle(t *testing.T) {
	c, srv := newClient(t)
	ctx := context.Background()
	jit, id, err := c.GenerateJITConfig(ctx, "acme/widgets", "mc-vm1-j5", []string{"self-hosted", "macos"}, "/Users/admin/_work")
	if err != nil {
		t.Fatal(err)
	}
	if jit == "" || id == 0 {
		t.Fatalf("bad jit result: %q %d", jit, id)
	}
	runners, err := c.ListRunners(ctx, "acme/widgets")
	if err != nil {
		t.Fatal(err)
	}
	if len(runners) != 1 || runners[0].Name != "mc-vm1-j5" {
		t.Fatalf("unexpected runners: %+v", runners)
	}
	if err := c.RemoveRunner(ctx, "acme/widgets", id); err != nil {
		t.Fatal(err)
	}
	if len(srv.RemovedIDs) != 1 {
		t.Fatal("runner not removed")
	}
}

func TestGetJobStatusAndQueuedDiscovery(t *testing.T) {
	c, srv := newClient(t)
	srv.SetJob(&ghfake.Job{ID: 10, RunID: 100, RunAttempt: 1, Status: "queued", Labels: []string{"self-hosted", "macos", "arm64", "mini-control"}})
	srv.SetJob(&ghfake.Job{ID: 11, RunID: 100, RunAttempt: 1, Status: "queued", Labels: []string{"self-hosted", "linux"}})

	st, err := c.GetJobStatus(context.Background(), githubapp.JobRef{Repo: "acme/widgets", JobID: 10})
	if err != nil || st.Status != "queued" {
		t.Fatalf("job status: %+v err=%v", st, err)
	}

	ours := []string{"self-hosted", "macos", "arm64", "mini-control"}
	refs, err := c.ListQueuedJobs(context.Background(), []string{"acme/widgets"}, ours)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].JobID != 10 {
		t.Fatalf("queued discovery wrong: %+v", refs)
	}
}

func TestListAccessibleRepos(t *testing.T) {
	c, srv := newClient(t)
	srv.Repos = []string{"acme/widgets", "acme/gadgets"}
	repos, err := c.ListAccessibleRepos(context.Background())
	if err != nil || len(repos) != 2 || repos[1].FullName != "acme/gadgets" {
		t.Fatalf("repo discovery: %v err=%v", repos, err)
	}
	if repos[0].PushedAt.IsZero() {
		t.Fatal("pushed_at not parsed")
	}
}

func TestResolveRunnerDownload(t *testing.T) {
	c, _ := newClient(t)
	v, u, err := c.ResolveRunnerDownload(context.Background())
	if err != nil || v != "2.325.0" {
		t.Fatalf("latest version: %q err=%v", v, err)
	}
	want := "actions-runner-osx-arm64-2.325.0.tar.gz"
	if !strings.HasSuffix(u, want) {
		t.Fatalf("asset url %q does not end with %q", u, want)
	}
}

func TestListRunnersPaginates(t *testing.T) {
	c, srv := newClient(t)
	for i := 0; i < 150; i++ {
		srv.AddRunner(&ghfake.Runner{Name: fmt.Sprintf("mc-x-%d", i), Status: "online", Busy: i == 149})
	}
	runners, err := c.ListRunners(context.Background(), "acme/widgets")
	if err != nil {
		t.Fatal(err)
	}
	if len(runners) != 150 {
		t.Fatalf("pagination lost runners: got %d, want 150", len(runners))
	}
	// The runner past page 1 must still be visible (busy-check safety).
	found := false
	for _, r := range runners {
		if r.Busy {
			found = true
		}
	}
	if !found {
		t.Fatal("busy runner beyond page 1 not visible")
	}
}

func TestLabelsSatisfiable(t *testing.T) {
	ours := []string{"self-hosted", "macOS", "ARM64", "mini-control"}
	cases := []struct {
		job  []string
		want bool
	}{
		{[]string{"self-hosted", "macos", "arm64"}, true},
		{[]string{"self-hosted", "macos", "arm64", "mini-control"}, true},
		{[]string{"self-hosted", "linux"}, false},
		{[]string{"ubuntu-latest"}, false},
		{nil, false},
	}
	for _, tc := range cases {
		if got := githubapp.LabelsSatisfiable(tc.job, ours); got != tc.want {
			t.Errorf("LabelsSatisfiable(%v) = %v, want %v", tc.job, got, tc.want)
		}
	}
}

func TestFailedStepsDenyList(t *testing.T) {
	st := githubapp.JobStatus{Steps: []githubapp.JobStep{
		{Name: "ok", Conclusion: "success"},
		{Name: "skipped", Conclusion: "skipped"},
		{Name: "neutral", Conclusion: "neutral"},
		{Name: "pending", Conclusion: ""},
		{Name: "boom", Conclusion: "failure"},
		{Name: "slow", Conclusion: "timed_out"},
		{Name: "stopped", Conclusion: "cancelled"},
	}}
	got := st.FailedSteps()
	want := []string{"boom", "slow", "stopped"}
	if len(got) != len(want) {
		t.Fatalf("FailedSteps = %v, want %v", got, want)
	}
}

func TestRemoveRunnerGoneIsSuccess(t *testing.T) {
	c, _ := newClient(t)
	if err := c.RemoveRunner(context.Background(), "acme/widgets", 9999); err != nil {
		t.Fatalf("404 on delete should be success, got %v", err)
	}
}

func TestReapable(t *testing.T) {
	cases := []struct {
		r    githubapp.Runner
		want bool
	}{
		{githubapp.Runner{Status: "online", Busy: false}, false},
		{githubapp.Runner{Status: "online", Busy: true}, false},
		{githubapp.Runner{Status: "offline", Busy: false}, true},
		{githubapp.Runner{Status: "offline", Busy: true}, false},
		{githubapp.Runner{Status: "something-new", Busy: false}, true}, // unknown fails toward cleanup
	}
	for _, tc := range cases {
		if got := tc.r.Reapable(); got != tc.want {
			t.Errorf("Reapable(%+v) = %v, want %v", tc.r, got, tc.want)
		}
	}
}

// Sibling queued jobs of an in_progress run (matrix workflows) must be
// discovered even though the run is no longer status=queued.
func TestSiblingQueuedJobDiscovery(t *testing.T) {
	c, srv := newClient(t)
	labels := []string{"self-hosted", "macos", "arm64", "mini-control"}
	srv.SetJob(&ghfake.Job{ID: 201, RunID: 2000, RunAttempt: 1, Status: "in_progress", Labels: labels})
	srv.SetJob(&ghfake.Job{ID: 202, RunID: 2000, RunAttempt: 1, Status: "queued", Labels: labels})

	refs, err := c.ListQueuedJobs(context.Background(), []string{"acme/widgets"}, labels)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].JobID != 202 {
		t.Fatalf("sibling discovery wrong: %+v", refs)
	}
}

// Steady-state polls must not refetch a run's job list when nothing changed.
func TestJobsFetchGatedByUpdatedAt(t *testing.T) {
	c, srv := newClient(t)
	labels := []string{"self-hosted", "macos", "arm64", "mini-control"}
	srv.SetJob(&ghfake.Job{ID: 301, RunID: 3000, RunAttempt: 1, Status: "queued", Labels: labels})

	if _, err := c.ListQueuedJobs(context.Background(), []string{"acme/widgets"}, labels); err != nil {
		t.Fatal(err)
	}
	first := srv.JobsFetchCount()
	if first == 0 {
		t.Fatal("first cycle fetched no jobs")
	}
	if _, err := c.ListQueuedJobs(context.Background(), []string{"acme/widgets"}, labels); err != nil {
		t.Fatal(err)
	}
	if srv.JobsFetchCount() != first {
		t.Fatal("unchanged run was refetched")
	}
	// A status change moves updated_at and must trigger a refetch.
	srv.SetJob(&ghfake.Job{ID: 301, RunID: 3000, RunAttempt: 1, Status: "in_progress", Labels: labels})
	srv.SetJob(&ghfake.Job{ID: 302, RunID: 3000, RunAttempt: 1, Status: "queued", Labels: labels})
	refs, err := c.ListQueuedJobs(context.Background(), []string{"acme/widgets"}, labels)
	if err != nil {
		t.Fatal(err)
	}
	if srv.JobsFetchCount() <= first {
		t.Fatal("changed run was not refetched")
	}
	if len(refs) != 1 || refs[0].JobID != 302 {
		t.Fatalf("new sibling not found after change: %+v", refs)
	}
}
