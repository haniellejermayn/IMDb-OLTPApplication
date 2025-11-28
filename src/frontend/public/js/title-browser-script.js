document.addEventListener("DOMContentLoaded", () => {
  // --------------------
  // Global state
  // --------------------
  let currentPage = 1;
  const limit = 20;
  let currentQuery = {
    q: "",
    year_from: "",
    year_to: "",
    genre: "",
    type: "",
  };

  const allMovies = Array.from({ length: 200 }, (_, i) => ({
    tconst: "tt" + (100000 + i + 1),
    primaryTitle: "Sample Movie " + (i + 1),
    startYear: 1980 + (i % 40),
    runtimeMinutes: (i % 120) + 60,
    genres: ["Action", "Drama", "Comedy"][i % 3],
    type: ["movie", "tvSeries", "short"][i % 3],
  }));

  // --------------------
  // Render function
  // --------------------
  function renderTitles(list) {
    const container = document.getElementById("titlesList");
    container.innerHTML = "";

    if (!list || list.length === 0) {
      container.innerHTML = `<p>No titles found.</p>`;
      return;
    }

    list.forEach((movie) => {
      container.innerHTML += `
        <div class="title-item">
          <div class="title-left">
            <span><strong>TCONST:</strong> ${movie.tconst}</span>
            <span class="title-name">${movie.primaryTitle}</span>
          </div>
          <div class="title-right">
            <span><strong>Year:</strong> ${movie.startYear}</span>
            <span><strong>Minutes:</strong> ${movie.runtimeMinutes}</span>
            <span><strong>Genre:</strong> ${movie.genres}</span>
          </div>
        </div>
      `;
    });

    document.getElementById("pageNumber").innerText = `Page ${currentPage}`;
  }

  // --------------------
  // Get filtered and randomized movies
  // --------------------
  function getFilteredMovies() {
    let filtered = [...allMovies];

    if (currentQuery.q)
      filtered = filtered.filter((m) =>
        m.primaryTitle.toLowerCase().includes(currentQuery.q.toLowerCase())
      );

    if (currentQuery.year_from)
      filtered = filtered.filter(
        (m) => m.startYear >= parseInt(currentQuery.year_from)
      );

    if (currentQuery.year_to)
      filtered = filtered.filter(
        (m) => m.startYear <= parseInt(currentQuery.year_to)
      );

    if (currentQuery.type)
      filtered = filtered.filter((m) => m.type === currentQuery.type);

    if (currentQuery.genre)
      filtered = filtered.filter((m) => m.genres === currentQuery.genre);

    // Randomize the filtered list
    return filtered.sort(() => 0.5 - Math.random());
  }

  // --------------------
  // Show current page
  // --------------------
  function showPage() {
    const filtered = getFilteredMovies();
    const start = (currentPage - 1) * limit;
    const pageItems = filtered.slice(start, start + limit);
    renderTitles(pageItems);
  }

  // --------------------
  // Search button
  // --------------------
  document.getElementById("searchBtn").addEventListener("click", () => {
    currentQuery.q = document.getElementById("searchInput").value;
    currentQuery.year_from = document.getElementById("yearFrom").value;
    currentQuery.year_to = document.getElementById("yearTo").value;
    currentQuery.type = document.getElementById("typeSelect").value;
    currentQuery.genre = document.getElementById("genreSelect").value;

    currentPage = 1;
    showPage();
  });

  // --------------------
  // Pagination buttons
  // --------------------
  document.getElementById("nextBtn").addEventListener("click", () => {
    currentPage++;
    showPage();
  });

  document.getElementById("prevBtn").addEventListener("click", () => {
    if (currentPage > 1) currentPage--;
    showPage();
  });

  // --------------------
  // Initial load: 20 random movies
  // --------------------
  showPage();
});
