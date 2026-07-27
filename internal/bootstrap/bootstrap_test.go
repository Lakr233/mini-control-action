package bootstrap

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestRenderScriptDownloadMode(t *testing.T) {
	script, err := RenderScript(Params{DownloadURL: "https://example.test/runner.tar.gz"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"#!/bin/sh",
		"https://example.test/runner.tar.gz",
		"--jitconfig",
		"PHASE=done",
		"fail download_failed",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q", want)
		}
	}
}

func TestRenderScriptPreinstalled(t *testing.T) {
	script, err := RenderScript(Params{PreinstalledPath: "/opt/runner"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(script, `RUNNER_DIR="/opt/runner"`) {
		t.Error("preinstalled path not templated")
	}
}

func TestStageCommandRoundTrips(t *testing.T) {
	script := "#!/bin/sh\necho 'tricky \"quotes\" $VARS `backticks`'\n"
	jit := base64.StdEncoding.EncodeToString([]byte("jit-blob"))
	cmd := StageCommand(script, jit)
	// The script must survive shell transport byte-exact via base64.
	sb64 := base64.StdEncoding.EncodeToString([]byte(script))
	if !strings.Contains(cmd, sb64) {
		t.Fatal("stage command does not embed script base64")
	}
	if !strings.Contains(cmd, jit) || !strings.Contains(cmd, "echo staged") {
		t.Fatal("stage command incomplete")
	}
}

func TestParseStatus(t *testing.T) {
	cases := []struct {
		in      string
		phase   string
		done    bool
		failed  bool
		exit    int
		hasExit bool
	}{
		{"", "", false, false, 0, false},
		{"PHASE=download\n---MCRA-LOG---\ndownloading...", "download", false, false, 0, false},
		{"PHASE=download_failed\nEXIT=1\n---MCRA-LOG---\ncurl: (6) err", "download_failed", false, true, 1, true},
		{"PHASE=run\n---MCRA-LOG---\nListening for Jobs", "run", false, false, 0, false},
		{"PHASE=done\nEXIT=0\n---MCRA-LOG---\nJob completed", "done", true, false, 0, true},
		{"PHASE=done\nEXIT=2\n---MCRA-LOG---\n", "done", true, false, 2, true},
	}
	for _, tc := range cases {
		st := ParseStatus(tc.in)
		if st.Phase != tc.phase || st.Done() != tc.done || st.Failed() != tc.failed || st.Exit != tc.exit || st.HasExit != tc.hasExit {
			t.Errorf("ParseStatus(%q) = %+v, want phase=%s done=%v failed=%v exit=%d", tc.in, st, tc.phase, tc.done, tc.failed, tc.exit)
		}
	}
	if ParseStatus("PHASE=run\n---MCRA-LOG---\nhello").LogTail != "hello" {
		t.Error("log tail not captured")
	}
}

func TestURLHelpers(t *testing.T) {
	if got := DefaultDownloadURL("2.325.0"); got != "https://github.com/actions/runner/releases/download/v2.325.0/actions-runner-osx-arm64-2.325.0.tar.gz" {
		t.Errorf("unexpected default url %s", got)
	}
	if got := MirrorURL("https://mirror.test/{version}/r.tgz", "2.1.0"); got != "https://mirror.test/2.1.0/r.tgz" {
		t.Errorf("unexpected mirror url %s", got)
	}
}
