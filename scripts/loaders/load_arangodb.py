"""
Load IMDb graph data into ArangoDB Oasis
Uses AQL (ArangoDB Query Language) via python-arango driver
"""

import os
import time
import pandas as pd
from arango import ArangoClient
from dotenv import load_dotenv
from pathlib import Path
import json

# Load environment variables
load_dotenv()

class ArangoDBLoader:
    def __init__(self):
        """Initialize ArangoDB connection"""
        self.url = os.getenv('ARANGO_URL')
        self.user = os.getenv('ARANGO_USER', 'root')
        self.password = os.getenv('ARANGO_PASSWORD')
        self.database_name = os.getenv('ARANGO_DATABASE', '_system')

        if not all([self.url, self.password]):
            raise ValueError("Missing ArangoDB credentials in .env file")

        print(f"Connecting to ArangoDB...")
        print(f"  URL: {self.url}")
        print(f"  User: {self.user}")
        print(f"  Database: {self.database_name}")

        self.client = ArangoClient(hosts=self.url)
        self.db = self.client.db(self.database_name, username=self.user, password=self.password)
        self.stats = {}

    def test_connection(self):
        """Test database connection"""
        try:
            version = self.db.version()
            print(f"Connected! ArangoDB version: {version}")
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def create_collections(self):
        """Create collections (like tables) for actors, movies, and edges"""
        print("\nCreating collections...")

        # Create actors collection
        if not self.db.has_collection('actors'):
            self.db.create_collection('actors')
            print("  Created 'actors' collection")
        else:
            print("  'actors' collection already exists")

        # Create movies collection
        if not self.db.has_collection('movies'):
            self.db.create_collection('movies')
            print("  Created 'movies' collection")
        else:
            print("  'movies' collection already exists")

        # Create edge collection for ACTED_IN relationships
        if not self.db.has_collection('acted_in'):
            self.db.create_collection('acted_in', edge=True)
            print("  Created 'acted_in' edge collection")
        else:
            print("  'acted_in' edge collection already exists")

    def create_indexes(self):
        """Create indexes for performance"""
        print("\nCreating indexes...")

        actors = self.db.collection('actors')
        movies = self.db.collection('movies')

        # Index on actor name
        try:
            actors.add_persistent_index(fields=['name'], unique=False)
            print("  Created index on actors.name")
        except:
            print("  Index on actors.name already exists")

        # Index on movie title and year
        try:
            movies.add_persistent_index(fields=['title'], unique=False)
            print("  Created index on movies.title")
        except:
            print("  Index on movies.title already exists")

        try:
            movies.add_persistent_index(fields=['year'], unique=False)
            print("  Created index on movies.year")
        except:
            print("  Index on movies.year already exists")

    def clear_collections(self):
        """Clear existing data (optional)"""
        print("\nClearing existing data...")
        self.db.collection('actors').truncate()
        self.db.collection('movies').truncate()
        self.db.collection('acted_in').truncate()
        print("Collections cleared")

    def load_actors(self, csv_path, batch_size=1000):
        """Load actors from CSV in batches"""
        print(f"\nLoading actors from {csv_path}...")
        df = pd.read_csv(csv_path)
        total = len(df)

        start_time = time.time()
        actors_collection = self.db.collection('actors')

        # Prepare documents
        actors = []
        for idx, row in df.iterrows():
            actors.append({
                '_key': str(int(row['actorId'])),  # Use actorId as document key
                'id': int(row['actorId']),
                'name': str(row['name'])
            })

            if len(actors) >= batch_size or idx == total - 1:
                actors_collection.import_bulk(actors)
                loaded = idx + 1
                print(f"  Progress: {loaded}/{total} actors ({loaded/total*100:.1f}%)", end='\r')
                actors = []

        elapsed = time.time() - start_time
        rate = total / elapsed

        print(f"\nLoaded {total:,} actors in {elapsed:.2f}s ({rate:.0f} actors/sec)")

        self.stats['actors'] = {
            'count': total,
            'time_seconds': elapsed,
            'rate_per_second': rate
        }

    def load_movies(self, csv_path, batch_size=1000):
        """Load movies from CSV in batches"""
        print(f"\nLoading movies from {csv_path}...")
        df = pd.read_csv(csv_path)
        total = len(df)

        start_time = time.time()
        movies_collection = self.db.collection('movies')

        # Prepare documents
        movies = []
        for idx, row in df.iterrows():
            movies.append({
                '_key': str(int(row['movieId'])),  # Use movieId as document key
                'id': int(row['movieId']),
                'title': str(row['title']),
                'year': int(row['year'])
            })

            if len(movies) >= batch_size or idx == total - 1:
                movies_collection.import_bulk(movies)
                loaded = idx + 1
                print(f"  Progress: {loaded}/{total} movies ({loaded/total*100:.1f}%)", end='\r')
                movies = []

        elapsed = time.time() - start_time
        rate = total / elapsed

        print(f"\nLoaded {total:,} movies in {elapsed:.2f}s ({rate:.0f} movies/sec)")

        self.stats['movies'] = {
            'count': total,
            'time_seconds': elapsed,
            'rate_per_second': rate
        }

    def load_roles(self, csv_path, batch_size=1000):
        """Load ACTED_IN relationships from CSV in batches"""
        print(f"\nLoading relationships from {csv_path}...")
        df = pd.read_csv(csv_path)
        total = len(df)

        start_time = time.time()
        acted_in_collection = self.db.collection('acted_in')

        # Prepare edge documents
        edges = []
        for idx, row in df.iterrows():
            edges.append({
                '_from': f"actors/{int(row['actorId'])}",
                '_to': f"movies/{int(row['movieId'])}"
            })

            if len(edges) >= batch_size or idx == total - 1:
                acted_in_collection.import_bulk(edges)
                loaded = idx + 1
                print(f"  Progress: {loaded}/{total} relationships ({loaded/total*100:.1f}%)", end='\r')
                edges = []

        elapsed = time.time() - start_time
        rate = total / elapsed

        print(f"\nLoaded {total:,} relationships in {elapsed:.2f}s ({rate:.0f} edges/sec)")

        self.stats['relationships'] = {
            'count': total,
            'time_seconds': elapsed,
            'rate_per_second': rate
        }

    def verify_data(self):
        """Verify loaded data"""
        print("\nVerifying loaded data...")

        # Count documents
        actor_count = self.db.collection('actors').count()
        movie_count = self.db.collection('movies').count()
        edge_count = self.db.collection('acted_in').count()

        print(f"  Actors: {actor_count:,}")
        print(f"  Movies: {movie_count:,}")
        print(f"  Relationships: {edge_count:,}")

        # Find Christian Bale
        aql = """
        FOR actor IN actors
            FILTER actor.name == 'Christian Bale'
            LET movie_count = LENGTH(
                FOR edge IN acted_in
                    FILTER edge._from == actor._id
                    RETURN 1
            )
            RETURN {actor: actor.name, movies: movie_count}
        """
        cursor = self.db.aql.execute(aql)
        result = list(cursor)
        if result:
            print(f"\n  Christian Bale found: {result[0]['movies']:,} movies")

        self.stats['verification'] = {
            'actors': actor_count,
            'movies': movie_count,
            'relationships': edge_count
        }

    def save_stats(self, output_path):
        """Save loading statistics"""
        self.stats['database'] = 'ArangoDB Oasis'
        self.stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')

        # Calculate total time
        total_time = sum(
            self.stats.get(key, {}).get('time_seconds', 0)
            for key in ['actors', 'movies', 'relationships']
        )
        self.stats['total_time_seconds'] = total_time

        with open(output_path, 'w') as f:
            json.dump(self.stats, f, indent=2)

        print(f"\nStatistics saved to {output_path}")

def main():
    # Paths
    data_dir = Path("C:/Users/hasann2/Downloads/graph-benchmark/data/processed")
    results_dir = Path("C:/Users/hasann2/Downloads/graph-benchmark/results")
    results_dir.mkdir(exist_ok=True)

    # Initialize loader
    loader = ArangoDBLoader()

    # Test connection
    if not loader.test_connection():
        print("Failed to connect. Check your .env file.")
        return

    # Create collections
    loader.create_collections()

    # Create indexes
    loader.create_indexes()

    # Optional: Clear existing data
    # Uncomment if you want to start fresh
    # loader.clear_collections()

    # Load data
    loader.load_actors(data_dir / 'actors.csv')
    loader.load_movies(data_dir / 'movies.csv')
    loader.load_roles(data_dir / 'roles.csv')

    # Verify
    loader.verify_data()

    # Save statistics
    loader.save_stats(results_dir / 'arangodb_loading_stats.json')

    print("\n" + "="*60)
    print("ArangoDB data loading complete!")
    print("="*60)

if __name__ == '__main__':
    main()
