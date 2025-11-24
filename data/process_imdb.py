import csv
import time
from datetime import datetime

def process_imdb():
    start_time = time.time()
    print(f"\nStarting data extraction at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    input_file = 'title.basics.tsv'
    output_all = 'node1_all_titles.csv'
    output_movies = 'node2_movies.csv'
    output_nonmovies = 'node3_non_movies.csv'

    count = 0
    type_counts = {'movie': 0, 'non_movie': 0}

    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_all, 'w', newline='', encoding='utf-8') as file_all, \
         open(output_movies, 'w', newline='', encoding='utf-8') as file_movies, \
         open(output_nonmovies, 'w', newline='', encoding='utf-8') as file_nonmovies:
        
        reader = csv.DictReader(infile, delimiter='\t')
        writer_all = csv.writer(file_all)
        writer_movies = csv.writer(file_movies)
        writer_nonmovies = csv.writer(file_nonmovies)
        
        header = ['tconst', 'title_type', 'primary_title', 'start_year', 'runtime_minutes', 'genres']
        writer_all.writerow(header)
        writer_movies.writerow(header)
        writer_nonmovies.writerow(header)
        
        for row in reader:
            # Filter: non-adult, valid year, has title, has runtime
            if (row['isAdult'] == '0' and 
                row['startYear'] != '\\N' and
                row['runtimeMinutes'] != '\\N' and 
                row['primaryTitle'] and
                int(row['startYear']) >= 1950 and
                count < 40000):
                
                # Clean data
                year = row['startYear']
                runtime = row['runtimeMinutes']
                genres = row['genres'].replace('\\N', 'Unknown')
                title = row['primaryTitle'].replace('"', '""')
                title_type = row['titleType']
                tconst = row['tconst']
                
                row_data = [tconst, title_type, title, year, runtime, genres]
                
                writer_all.writerow(row_data)
                
                if title_type == 'movie':
                    writer_movies.writerow(row_data)
                    type_counts['movie'] += 1
                else:
                    writer_nonmovies.writerow(row_data)
                    type_counts['non_movie'] += 1
                
                count += 1
                
                # Progress indicator every 10,000 rows
                if count % 10000 == 0:
                    print(f"Processed {count} rows...")
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    print("=" * 60)
    print("DATA EXTRACTION COMPLETE")
    print("=" * 60)
    print(f"Total processed: {count} rows")
    print(f"Processing time: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)")
    print(f"\nNode 1 (Central - All titles): {count} rows")
    print(f"Node 2 (Movies only): {type_counts['movie']} rows ({type_counts['movie']/count*100:.1f}%)")
    print(f"Node 3 (Non-movies): {type_counts['non_movie']} rows ({type_counts['non_movie']/count*100:.1f}%)")
    print(f"\nVerification: {type_counts['movie']} + {type_counts['non_movie']} = {type_counts['movie'] + type_counts['non_movie']}")
    print(f"Should equal total: {count} ✓" if (type_counts['movie'] + type_counts['non_movie']) == count else f"Should equal total: {count} ✗ ERROR!")
    print("=" * 60)
    print("Files created:")
    print(f"  - {output_all}")
    print(f"  - {output_movies}")
    print(f"  - {output_nonmovies}")
    print("=" * 60)

if __name__ == '__main__':
    process_imdb()