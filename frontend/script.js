document.addEventListener('DOMContentLoaded', () => {
  const form = document.getElementById('recommender-form');
  const terminalOutput = document.getElementById('terminal-output');
  const executeBtn = document.getElementById('execute-btn');

  // Multi-select Logic
  const ALL_GENRES = ["Action", "Animation", "Biography", "Adventure", "Anime", "Children", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "History", "Horror", "Kids", "Musical", "Mystery", "Romance", "Sci-Fi", "Science Fiction", "Short", "Sport", "Superhero", "Suspense", "Thriller", "TV Movie", "War", "Western"];
  let selectedGenres = [];

  const genreSearch = document.getElementById('genre-search');
  const genreDropdown = document.getElementById('genre-dropdown');
  const selectedGenresTags = document.getElementById('selected-genres-tags');

  function renderDropdown(filterText = '') {
    genreDropdown.innerHTML = '';
    const availableGenres = ALL_GENRES.filter(g => !selectedGenres.includes(g) && g.toLowerCase().includes(filterText.toLowerCase()));
    
    availableGenres.forEach(genre => {
      const item = document.createElement('div');
      item.className = 'dropdown-item';
      item.textContent = genre;
      item.addEventListener('click', () => {
        addGenre(genre);
        genreSearch.value = '';
        renderDropdown();
        genreSearch.focus();
      });
      genreDropdown.appendChild(item);
    });

    if (availableGenres.length === 0) {
      const item = document.createElement('div');
      item.className = 'dropdown-item';
      item.textContent = 'No matching genres';
      item.style.pointerEvents = 'none';
      item.style.color = 'var(--text-dim)';
      genreDropdown.appendChild(item);
    }
  }

  function renderTags() {
    selectedGenresTags.innerHTML = '';
    selectedGenres.forEach(genre => {
      const tag = document.createElement('div');
      tag.className = 'cyber-tag';
      tag.innerHTML = `<span>${genre}</span><span class="tag-remove">&times;</span>`;
      
      tag.querySelector('.tag-remove').addEventListener('click', (e) => {
        e.stopPropagation();
        removeGenre(genre);
      });
      
      selectedGenresTags.appendChild(tag);
    });
  }

  function addGenre(genre) {
    if (!selectedGenres.includes(genre)) {
      selectedGenres.push(genre);
      renderTags();
    }
  }

  function removeGenre(genre) {
    selectedGenres = selectedGenres.filter(g => g !== genre);
    renderTags();
    renderDropdown(genreSearch.value);
  }

  genreSearch.addEventListener('focus', () => {
    renderDropdown(genreSearch.value);
    genreDropdown.classList.add('active');
  });

  genreSearch.addEventListener('input', (e) => {
    renderDropdown(e.target.value);
    genreDropdown.classList.add('active');
  });

  document.addEventListener('click', (e) => {
    if (!e.target.closest('#genre-select-group')) {
      genreDropdown.classList.remove('active');
    }
  });

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
      if (!res.ok) throw new Error('Server returned ' + res.status);
      return res.json();
    })
    .then(data => {
      if (data.output) {
        const lines = data.output.split('\n');
        lines.forEach(l => {
          if (l.trim()) addLog(l, 'info');
        });
      }
      
      if (data.error) {
        const errLines = data.error.split('\n');
        errLines.forEach(l => {
          if (l.trim()) addLog(l, 'error');
        });
      }
      
      if (data.success) {
        addLog(`\n[ SUCCESS ] Sequence completed successfully.`, 'success');
      } else {
        addLog(`\n[ ERROR ] Sequence failed.`, 'error');
      }
      
      // Show real chosen movies from the report
      if (data.movies && data.movies.length > 0) {
        addLog(`\n[ CHOSEN MOVIES — ${data.movies.length} items ]`, 'warning');
        data.movies.forEach(m => {
          addLog(`${m.title} (${m.year}) - ${m.genre}`, 'info');
        });
        addLog('');
      } else {
        addLog(`\n[ CHOSEN MOVIES ] No movies returned.`, 'warning');
      }
      
    })
    .catch(err => {
      addLog(`[ERROR] Failed to communicate with server: ${err.message}`, 'error');
      addLog('Are you sure the Python server (server.py) is running?', 'warning');
    })
    .finally(() => {
      // Re-enable button
      executeBtn.disabled = false;
      executeBtn.style.opacity = '1';
    });
  });
});
