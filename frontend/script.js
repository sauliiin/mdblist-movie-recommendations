document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('recommender-form');
  const terminalOutput = document.getElementById('terminal-output');
  const executeBtn = document.getElementById('execute-btn');

  // Genre toggle grid
  const ALL_GENRES = ["Action", "Animation", "Biography", "Adventure", "Anime", "Children", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "History", "Horror", "Kids", "Musical", "Mystery", "Romance", "Sci-Fi", "Science Fiction", "Short", "Sport", "Superhero", "Suspense", "Thriller", "TV Movie", "War", "Western"];
  let selectedGenres = [];

  const genreGrid = document.getElementById('genre-grid');

  function renderGenreGrid() {
    genreGrid.innerHTML = '';
    ALL_GENRES.forEach(genre => {
      const isSelected = selectedGenres.includes(genre);
      const chip = document.createElement('div');
      chip.className = 'genre-chip' + (isSelected ? ' selected' : '');
      chip.innerHTML = `<span>${genre}</span>` + (isSelected ? '<span class="chip-mark">&times;</span>' : '');
      chip.addEventListener('click', () => toggleGenre(genre));
      genreGrid.appendChild(chip);
    });
  }

  function toggleGenre(genre) {
    if (selectedGenres.includes(genre)) {
      selectedGenres = selectedGenres.filter(g => g !== genre);
    } else {
      selectedGenres.push(genre);
    }
    renderGenreGrid();
  }

  renderGenreGrid();

  // Field Validation Logic
  const imdbInputs = [document.getElementById('imdb-min'), document.getElementById('imdb-max')];
  imdbInputs.forEach(input => {
    input.addEventListener('blur', (e) => {
      let val = e.target.value.trim();
      if (!val) return;
      
      // Replace comma with dot
      val = val.replace(',', '.');
      
      // Parse float
      let num = parseFloat(val);
      if (isNaN(num)) {
        e.target.value = '';
        return;
      }
      
      // Enforce 0 to 10
      if (num < 0) num = 0;
      if (num > 10) num = 10;
      
      // Format to 1 decimal place
      e.target.value = num.toFixed(1);
    });
  });

  const votesInput = document.getElementById('imdb-min-votes');
  votesInput.addEventListener('blur', (e) => {
    let val = e.target.value.trim();
    if (!val) return;
    
    let num = parseInt(val, 10);
    if (isNaN(num)) {
      e.target.value = '';
      return;
    }
    
    if (num < 0) num = 0;
    e.target.value = num;
  });

  const currentYear = new Date().getFullYear();
  const yearInputs = [document.getElementById('year-min'), document.getElementById('year-max')];
  yearInputs.forEach(input => {
    input.addEventListener('blur', (e) => {
      let val = e.target.value.trim();
      if (!val) return;
      
      let num = parseInt(val, 10);
      if (isNaN(num)) {
        e.target.value = '';
        return;
      }
      
      if (num < 1900) num = 1900;
      if (num > currentYear) num = currentYear;
      
      e.target.value = num;
    });
  });

  // Add line to terminal
  function addLog(message, type = 'info') {
    const logLine = document.createElement('div');
    logLine.className = `log-line ${type}`;
    logLine.textContent = message;
    
    // Insert before cursor
    const cursor = terminalOutput.querySelector('.cursor');
    if (cursor) {
      terminalOutput.insertBefore(logLine, cursor);
    } else {
      terminalOutput.appendChild(logLine);
    }
    
    // Auto-scroll
    terminalOutput.scrollTop = terminalOutput.scrollHeight;
  }

  // Handle form submission
  form.addEventListener('submit', (e) => {
    e.preventDefault();
    
    // Extract values
    const genres = selectedGenres.join(',');
    const keywords = document.getElementById('exclude-keywords').value.trim();
    const actors = document.getElementById('exclude-actors').value.trim();
    const imdbMin = document.getElementById('imdb-min').value;
    const imdbMax = document.getElementById('imdb-max').value;
    const imdbMinVotes = document.getElementById('imdb-min-votes').value;
    const yearMin = document.getElementById('year-min').value;
    const yearMax = document.getElementById('year-max').value;
    const dryRun = document.getElementById('dry-run').checked;

    // Build command
    let command = 'python3 recommended_for_jedi.py';
    
    if (genres) command += ` \\\n  --exclude-genres "${genres}"`;
    if (keywords) command += ` \\\n  --exclude-keywords "${keywords}"`;
    if (actors) command += ` \\\n  --exclude-actors "${actors}"`;
    if (imdbMin) command += ` \\\n  --imdb-min ${imdbMin}`;
    if (imdbMax) command += ` \\\n  --imdb-max ${imdbMax}`;
    if (imdbMinVotes) command += ` \\\n  --imdb-min-votes ${imdbMinVotes}`;
    if (yearMin) command += ` \\\n  --year-min ${yearMin}`;
    if (yearMax) command += ` \\\n  --year-max ${yearMax}`;
    if (dryRun) command += ` \\\n  --dry-run`;

    // Disable button during execution
    executeBtn.disabled = true;
    executeBtn.style.opacity = '0.5';

    // Execution sequence
    addLog('');
    addLog(`Initiating sequence...`, 'sys');
    addLog(`Executing command:`, 'sys');
    
    const cmdLines = command.split('\n');
    cmdLines.forEach(line => addLog(line, 'cmd'));
    
    addLog(`Connecting to backend server...`, 'sys');

    const payload = {
      genres, keywords, actors, imdbMin, imdbMax, imdbMinVotes, yearMin, yearMax, dryRun
    };

    fetch('/api/run', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    })
    .then(res => {
      if (res.status === 409) {
        throw new Error('A job is already running on the server.');
      }
      if (!res.ok) throw new Error('Server returned ' + res.status);
      return res.json();
    })
    .then(() => {
      addLog(`Job started. Streaming live output...`, 'sys');
      pollStatus();
    })
    .catch(err => {
      addLog(`[ERROR] Failed to communicate with server: ${err.message}`, 'error');
      addLog('Are you sure the Python server (server.py) is running?', 'warning');
      executeBtn.disabled = false;
      executeBtn.style.opacity = '1';
    });
  });

  // Poll the backend for incremental log lines while the job is running
  let offset = 0;
  function pollStatus() {
    fetch(`/api/status?offset=${offset}`)
      .then(res => {
        if (!res.ok) throw new Error('Server returned ' + res.status);
        return res.json();
      })
      .then(data => {
        data.log.forEach(line => {
          if (line.trim()) addLog(line, 'info');
        });
        offset = data.total;

        if (data.running) {
          setTimeout(pollStatus, 1000);
          return;
        }

        // Job finished
        if (data.success) {
          addLog(`\n[ SUCCESS ] Sequence completed successfully.`, 'success');
        } else {
          addLog(`\n[ ERROR ] Sequence failed.`, 'error');
        }

        if (data.movies && data.movies.length > 0) {
          addLog(`\n[ CHOSEN MOVIES — ${data.movies.length} items ]`, 'warning');
          data.movies.forEach(m => {
            addLog(`${m.title} (${m.year}) - ${m.genre}`, 'info');
          });
          addLog('');
        } else {
          addLog(`\n[ CHOSEN MOVIES ] No movies returned.`, 'warning');
        }

        offset = 0;
        executeBtn.disabled = false;
        executeBtn.style.opacity = '1';
      })
      .catch(err => {
        addLog(`[ERROR] Lost connection while polling: ${err.message}`, 'error');
        executeBtn.disabled = false;
        executeBtn.style.opacity = '1';
      });
  }
});
