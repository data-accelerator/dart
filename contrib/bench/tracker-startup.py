#!/usr/bin/env python3
"""Compare startup publication/refresh policies using temporary Go overlays."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--count', type=int, default=3)
parser.add_argument('--policy', choices=['all', 'frozen', 'normal', 'once', 'backoff'], default='all')
parser.add_argument('--scenario', choices=['burst', 'staggered', 'reverse', 'slow-growth'])
args = parser.parse_args()
if args.count < 1:
    parser.error('--count must be positive')
repo = Path(__file__).resolve().parents[2]
reader_path = repo / 'internal/engine/readers.go'
tracker_path = repo / 'internal/tracker/tracker.go'
readers = reader_path.read_text()
start = readers.index('func readerRefreshDelay(')
end = readers.index('\n// trackerAddr', start)
normal = readers[:start] + '''func readerRefreshDelay(previous, ceiling time.Duration) time.Duration {
    return ceiling
}
''' + readers[end:]
once = readers[:start] + '''func readerRefreshDelay(previous, ceiling time.Duration) time.Duration {
    if previous <= 0 { return min(initialReaderRefresh, ceiling) }
    return ceiling
}
''' + readers[end:]
tracker = tracker_path.read_text()
start = tracker.index('\t// Growth must be visible in this JOIN response.')
end = tracker.index('\n\treturn JoinResponse{', start)
frozen = tracker[:start] + tracker[end:]
policies = ['frozen', 'normal', 'once', 'backoff'] if args.policy == 'all' else [args.policy]
with tempfile.TemporaryDirectory(prefix='dart-startup-overlay-') as tmp:
    tmp = Path(tmp)
    for policy in policies:
        replacements = {}
        if policy != 'backoff':
            candidate = tmp / 'readers.go'
            candidate.write_text(once if policy == 'once' else normal)
            replacements[str(reader_path)] = str(candidate)
        if policy == 'frozen':
            candidate = tmp / 'tracker.go'
            candidate.write_text(frozen)
            replacements[str(tracker_path)] = str(candidate)
        overlay = tmp / 'overlay.json'
        overlay.write_text(json.dumps({'Replace': replacements}))
        env = dict(os.environ, DART_TRACKER_STARTUP_EXPERIMENT='1', DART_REFRESH_POLICY=policy)
        env.setdefault('GOMAXPROCS', '2')
        pattern = '^TestTrackerStartupExperiment$'
        if args.scenario:
            pattern += '/^' + args.scenario + '$'
        subprocess.run(['go', 'test', '-overlay', str(overlay), './internal/engine',
                        '-run', pattern, '-count', str(args.count), '-timeout', '5m', '-v'],
                       cwd=repo, env=env, check=True)
