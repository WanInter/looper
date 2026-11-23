package runtime

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/nexu-io/looper/internal/config"
	"github.com/nexu-io/looper/internal/coordinator"
	"github.com/nexu-io/looper/internal/storage"
)

func TestSchedulerDeliveryLaneIndependentOfTriage(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	db := openMigratedCoordinator(t, filepath.Join(root, "state.sqlite"), t.TempDir())
	repos := storage.NewRepositories(db.DB())
	insertSchedulerProject(t, repos, root, formatJavaScriptISOString(time.Now().UTC()))
	cfg, err := config.DefaultConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Projects = []config.ProjectRefConfig{{ID: "looper", Repo: "nexu-io/looper"}}
	cfg.Roles.Worker.AutoDiscovery = true
	cfg.Roles.Reviewer.Behavior.ReviewEvents.AutoImplement = true
	calls := 0
	input := defaultSchedulerTickInput{Config: &cfg, Repos: repos, CoordinatorEnabled: func(string) bool { return false }, Now: time.Now,
		ReconcileDeliveries: func(_ context.Context, in coordinator.DiscoveryInput) error {
			if in.ProjectID != "looper" || in.Repo != "nexu-io/looper" || in.Snapshot != nil {
				t.Fatalf("unexpected delivery scope: %#v", in)
			}
			calls++
			return nil
		},
	}
	for _, tc := range []struct {
		mode                  config.NetworkMode
		worker, autoImplement bool
		want                  int
	}{{"", true, true, 1}, {"", false, true, 0}, {"", true, false, 0}, {config.NetworkModeRouted, true, true, 0}} {
		cfg.Projects[0].Network.Mode = tc.mode
		cfg.Roles.Worker.AutoDiscovery = tc.worker
		cfg.Roles.Reviewer.Behavior.ReviewEvents.AutoImplement = tc.autoImplement
		calls = 0
		if err := runDefaultSchedulerTick(ctx, input); err != nil || calls != tc.want {
			t.Fatalf("tick(%+v): calls=%d err=%v", tc, calls, err)
		}
	}
}
