// Package bootstrap builds the shell commands that install and launch an
// ephemeral GitHub Actions runner inside a mini-control macOS VM through the
// ssh/exec channel. The channel has no stdin, a 10-minute server-side timeout
// and 1 MiB output caps, so the protocol is three cheap execs:
//
//  1. Stage: deliver the bootstrap script and JIT config base64-inline.
//  2. Launch: nohup the script detached so no exec waits on the job.
//  3. Poll: read a tiny status file + log tail until PHASE=done.
package bootstrap

import (
	"bufio"
	_ "embed"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"text/template"
)

//go:embed scripts/bootstrap.sh.tmpl
var scriptTmpl string

const (
	scriptPath = "$HOME/mcra-bootstrap.sh"
	jitPath    = "$HOME/.mcra-jit.b64"
	statusPath = "$HOME/.mcra-status"
	logPath    = "$HOME/mcra.log"
)

type Params struct {
	// DownloadURL is the fully-resolved runner tarball URL (osx-arm64).
	DownloadURL string
	// PreinstalledPath, when set, skips download and uses the runner already
	// present in the base image.
	PreinstalledPath string
}

// RenderScript produces the bootstrap script body.
func RenderScript(p Params) (string, error) {
	t, err := template.New("bootstrap").Parse(scriptTmpl)
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	err = t.Execute(&sb, map[string]string{
		"DownloadURL":      p.DownloadURL,
		"PreinstalledPath": p.PreinstalledPath,
		"StatusFile":       statusPath,
		"JITFile":          jitPath,
	})
	if err != nil {
		return "", err
	}
	return sb.String(), nil
}

// StageCommand writes the script and JIT blob into the VM. Everything is
// base64 so no quoting of script content is ever interpreted by the remote
// shell. jitConfig is GitHub's encoded_jit_config (itself base64; stored
// verbatim and passed to run.sh --jitconfig).
func StageCommand(script, jitConfig string) string {
	sb64 := base64.StdEncoding.EncodeToString([]byte(script))
	return fmt.Sprintf(
		"printf '%%s' %s | base64 -d > %s && printf '%%s' %s > %s && chmod 700 %s && rm -f %s && echo staged",
		shellQuote(sb64), scriptPath, shellQuote(jitConfig), jitPath, scriptPath, statusPath)
}

// LaunchCommand starts the bootstrap detached; the exec returns immediately.
func LaunchCommand() string {
	return fmt.Sprintf("nohup %s > %s 2>&1 & echo launched", scriptPath, logPath)
}

// StatusCommand reads the status file plus a small log tail (safely under the
// 1 MiB exec output cap).
func StatusCommand() string {
	return fmt.Sprintf("cat %s 2>/dev/null; echo '---MCRA-LOG---'; tail -c 4096 %s 2>/dev/null", statusPath, logPath)
}

// Status is the parsed content of the in-VM status file.
type Status struct {
	Phase   string // "", download, extract, run, done, *_failed
	Exit    int
	HasExit bool
	LogTail string
}

// Launched reports whether the bootstrap has started at all.
func (s Status) Launched() bool { return s.Phase != "" }

// Failed reports a bootstrap-level failure (before or while starting the runner).
func (s Status) Failed() bool { return strings.HasSuffix(s.Phase, "_failed") }

// Done reports the runner process has exited.
func (s Status) Done() bool { return s.Phase == "done" }

// ParseStatus parses StatusCommand output.
func ParseStatus(out string) Status {
	st := Status{}
	head, tail, found := strings.Cut(out, "---MCRA-LOG---")
	if found {
		st.LogTail = strings.TrimSpace(tail)
	}
	sc := bufio.NewScanner(strings.NewReader(head))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if v, ok := strings.CutPrefix(line, "PHASE="); ok {
			st.Phase = v
		}
		if v, ok := strings.CutPrefix(line, "EXIT="); ok {
			if n, err := strconv.Atoi(v); err == nil {
				st.Exit = n
				st.HasExit = true
			}
		}
	}
	return st
}

// AssetName is the release-asset filename for our target platform; this
// package owns platform naming.
func AssetName(version string) string {
	return fmt.Sprintf("actions-runner-osx-arm64-%s.tar.gz", version)
}

// DefaultDownloadURL builds the official runner tarball URL for a version
// like "2.325.0".
func DefaultDownloadURL(version string) string {
	return fmt.Sprintf("https://github.com/actions/runner/releases/download/v%s/%s", version, AssetName(version))
}

// MirrorURL substitutes {version} into a configured mirror template.
func MirrorURL(tmpl, version string) string {
	return strings.ReplaceAll(tmpl, "{version}", version)
}

// shellQuote single-quotes s for /bin/sh (base64 alphabets never contain ',
// but quote defensively anyway).
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
