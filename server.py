import http.server
import socketserver
import json
import subprocess
import threading
import glob
import urllib.parse

PORT = 8555
DIRECTORY = "frontend"

JOB_LOCK = threading.Lock()
JOB = {
    "running": False,
    "log": [],
    "success": None,
    "movies": [],
    "warnings": [],
    "command": "",
}


def collect_movies():
    movies = []
    warnings = []
    report_files = sorted(glob.glob("reports/recommended_for_jedi_*.json"))
    for rpath in reversed(report_files):
        try:
            with open(rpath, "r") as rf:
                report = json.load(rf)
            recs = report.get("recommendations", [])
            if not recs:
                continue
            for m in recs:
                genres = m.get("genres", [])
                movies.append({
                    "title": m.get("title", "Unknown"),
                    "year": m.get("year", "?"),
                    "genre": genres[0] if genres else "N/A"
                })
            warnings = report.get("warnings", [])
            break
        except Exception:
            continue
    return movies, warnings


def run_job(command):
    with JOB_LOCK:
        JOB["running"] = True
        JOB["log"] = []
        JOB["success"] = None
        JOB["movies"] = []
        JOB["warnings"] = []
        JOB["command"] = " ".join(command)

    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    for line in iter(proc.stdout.readline, ""):
        line = line.rstrip("\n")
        with JOB_LOCK:
            JOB["log"].append(line)
    proc.stdout.close()
    returncode = proc.wait()

    movies, warnings = collect_movies()
    with JOB_LOCK:
        JOB["running"] = False
        JOB["success"] = returncode == 0
        JOB["movies"] = movies
        JOB["warnings"] = warnings


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/status":
            query = urllib.parse.parse_qs(parsed.query)
            offset = int(query.get("offset", ["0"])[0])
            with JOB_LOCK:
                new_lines = JOB["log"][offset:]
                total = len(JOB["log"])
                response_data = {
                    "log": new_lines,
                    "total": total,
                    "running": JOB["running"],
                    "success": JOB["success"],
                    "movies": JOB["movies"] if not JOB["running"] else [],
                    "warnings": JOB["warnings"] if not JOB["running"] else [],
                    "command": JOB["command"],
                }
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response_data).encode('utf-8'))
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == '/api/run':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)

            try:
                params = json.loads(post_data)

                with JOB_LOCK:
                    already_running = JOB["running"]

                if already_running:
                    self.send_response(409)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "A job is already running."}).encode('utf-8'))
                    return

                # Build the command
                command = ["python3", "recommended_for_jedi.py", "--api-key", "omqfcrbt1dm8hj98mwuvgpg9n"]

                if params.get("genres"):
                    command.extend(["--exclude-genres", params["genres"]])
                if params.get("keywords"):
                    command.extend(["--exclude-keywords", params["keywords"]])
                if params.get("actors"):
                    command.extend(["--exclude-actors", params["actors"]])
                if params.get("imdbMin"):
                    command.extend(["--imdb-min", str(params["imdbMin"])])
                if params.get("imdbMax"):
                    command.extend(["--imdb-max", str(params["imdbMax"])])
                if params.get("imdbMinVotes"):
                    command.extend(["--imdb-min-votes", str(params["imdbMinVotes"])])
                if params.get("yearMin"):
                    command.extend(["--year-min", str(params["yearMin"])])
                if params.get("yearMax"):
                    command.extend(["--year-max", str(params["yearMax"])])
                if params.get("dryRun"):
                    command.append("--dry-run")

                thread = threading.Thread(target=run_job, args=(command,), daemon=True)
                thread.start()

                response_data = {"started": True, "command": " ".join(command)}
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response_data).encode('utf-8'))

            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        if "/api/status" in (self.path or ""):
            return
        super().log_message(format, *args)

class TCPServerReuse(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

if __name__ == "__main__":
    with TCPServerReuse(("", PORT), Handler) as httpd:
        print(f"Server initialized. Open http://localhost:{PORT} in your browser to access the frontend.")
        print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server.")
            httpd.server_close()
