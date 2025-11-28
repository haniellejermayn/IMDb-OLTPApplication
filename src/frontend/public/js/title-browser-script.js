/* --------------------------
            SAMPLE DATA (for demo)
           Replace with fetch() later
        --------------------------- */
const allMovies = [];
for (let i = 1; i <= 200; i++) {
  allMovies.push({
    tconst: "tt" + String(100000 + i),
    primaryTitle: "Sample Movie " + i,
    startYear: 1980 + (i % 40),
    runtimeMinutes: i % 150,
    genres: ["Action", "Drama", "Comedy"][i % 3],
  });
}

let filteredMovies = [...allMovies];
let currentPage = 1;
const pageSize = 20;

/* --------------------------
           RANDOMIZE & DISPLAY PAGE
        --------------------------- */
function getRandomPageItems() {
  const copy = [...filteredMovies];
  return copy.sort(() => Math.random() - 0.5).slice(0, pageSize);
}

function renderTitles() {
  const list = document.getElementById("titlesList");
  list.innerHTML = "";

  const pageItems = getRandomPageItems();

  pageItems.forEach((movie) => {
    list.innerHTML += `
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
}

/* --------------------------
            SEARCH FILTER
        --------------------------- */
function filterTitles() {
  const query = document.getElementById("searchInput").value.toLowerCase();
  filteredMovies = allMovies.filter((m) =>
    m.primaryTitle.toLowerCase().includes(query)
  );
  currentPage = 1;
  renderTitles();
}

/* --------------------------
            PAGINATION BUTTONS
        --------------------------- */
function nextPage() {
  currentPage++;
  renderTitles();
}

function prevPage() {
  if (currentPage > 1) currentPage--;
  renderTitles();
}

/* Initial Render */
renderTitles();
