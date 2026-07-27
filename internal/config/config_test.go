package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const minimal = `
github:
  owner: acme
  repo: widgets
  auth: { token: "ghp_test" }
minicontrol:
  base_url: https://example.test/api/client/v1
  api_key: "${TEST_MC_KEY}"
  sku: m4-big-v1
`

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadDefaultsAndEnvExpansion(t *testing.T) {
	t.Setenv("TEST_MC_KEY", "mck_abc")
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MiniControl.APIKey != "mck_abc" {
		t.Errorf("env expansion failed: %+v", cfg)
	}
	if cfg.Server.Listen != ":8080" {
		t.Errorf("server defaults wrong: %+v", cfg.Server)
	}
	if cfg.MiniControl.PollInterval.D() != 10*time.Second || cfg.MiniControl.ProvisionTimeout.D() != 12*time.Minute {
		t.Errorf("minicontrol defaults wrong: %+v", cfg.MiniControl)
	}
	if cfg.Limits.MaxConcurrentVMs != 4 || cfg.Runner.NamePrefix != "mc" {
		t.Errorf("defaults wrong: %+v %+v", cfg.Limits, cfg.Runner)
	}
	if len(cfg.Runner.Labels) == 0 || cfg.Runner.Labels[0] != "self-hosted" {
		t.Errorf("label defaults wrong: %v", cfg.Runner.Labels)
	}
}

func TestValidateRejects(t *testing.T) {
	t.Setenv("TEST_MC_KEY", "mck_abc")
	cases := []struct {
		name    string
		mutate  string
		wantErr string
	}{
		{"bad key prefix", strings.Replace(minimal, "${TEST_MC_KEY}", "notakey", 1), "mck_-prefixed"},
		{"missing sku", strings.Replace(minimal, "sku: m4-big-v1", "sku: \"\"", 1), "sku is required"},
		{"unknown field", minimal + "\nnot_a_field: true\n", "not_a_field"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.mutate))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestAuthTokenRequired(t *testing.T) {
	t.Setenv("TEST_MC_KEY", "mck_abc")
	missing := strings.Replace(minimal, `auth: { token: "ghp_test" }`, "auth: {}", 1)
	if _, err := Load(writeConfig(t, missing)); err == nil || !strings.Contains(err.Error(), "token is required") {
		t.Fatalf("want token-required error, got %v", err)
	}
	// The removed GitHub App block must now be rejected as an unknown field.
	app := strings.Replace(minimal, `auth: { token: "ghp_test" }`,
		"auth:\n    app: { app_id: 1, installation_id: 2, private_key_path: /k.pem }", 1)
	if _, err := Load(writeConfig(t, app)); err == nil || !strings.Contains(err.Error(), "app") {
		t.Fatalf("want unknown-field error for app block, got %v", err)
	}
}
