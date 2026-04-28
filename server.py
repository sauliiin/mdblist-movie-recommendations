import http.server
import socketserver
import json
import subprocess
import os

PORT = 8555
DIRECTORY = "frontend"

class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

    def do_POST(self):
        if self.path == '/api/run':
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            
            try:
                params = json.loads(post_data)
                
                # Build the command
                command = ["python3", "recommended_for_jedi.py", "--api-key", "your_api_here"]
                
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
                
                # Run the script
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True
                )
                
                # Read the latest report to get real movie recommendations
                movies = []
                import glob
                report_files = sorted(glob.glob("reports/recommended_for_jedi_*.json"))
                # Walk backwards through reports until we find one with recommendations
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
                        break  # Found a report with movies, stop looking
                    except Exception:
                        continue
                
                response_data = {
                    "success": result.returncode == 0,
                    "output": result.stdout,
                    "error": result.stderr,
                    "command": " ".join(command),
                    "movies": movies
                }
                
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

class TCPServerReuse(socketserver.TCPServer):
    allow_reuse_address = True

if __name__ == "__main__":
    with TCPServerReuse(("", PORT), Handler) as httpd:
        print(f"Server initialized. Open http://localhost:{PORT} in your browser to access the frontend.")
        print("Press Ctrl+C to stop.")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server.")
            httpd.server_close()
