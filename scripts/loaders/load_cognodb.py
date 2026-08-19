"""
Load IMDb graph data into CognoDB Cloud
Uses Cypher queries via Neo4j driver (CognoDB is Cypher-compatible)
"""

import os
import time
import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
from pathlib import Path
import json

# Load environment variables
load_dotenv()

class CognoDBLoader:
    def __init__(self):
        """Initialize CognoDB connection"""
        self.uri = os.getenv('COGNODB_URI')
        self.user = os.getenv('COGNODB_USER', 'cognodb')
        self.password = os.getenv('COGNODB_PASSWORD')

        if not all([self.uri, self.password]):
            raise ValueError("Missing CognoDB credentials in .env file")

        print(f"Connecting to CognoDB...")
        print(f"  URI: {self.uri}")
        print(f"  User: {self.user}")

        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
        self.stats = {}

    def test_connection(self):
        """Test database connection"""
        try:
            with self.driver.session() as session:
                result = session.run("RETURN 'Connected!' as message")
                message = result.single()["message"]
                print(f"✓ {message}")
                return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            return False

    def clear_database(self):
        """Clear existing data (optional - use with caution!)"""
        print("\nClearing existing data...")
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("✓ Database cleared")

    def create_indexes(self):
        """Create indexes for performance"""
        print("\nCreating indexes...")
        with self.driver.session() as session:
            # Indexes for faster lookups
            session.run("CREATE INDEX actor_id IF NOT EXISTS FOR (a:Actor) ON (a.id)")
            session.run("CREATE INDEX actor_name IF NOT EXISTS FOR (a:Actor) ON (a.name)")
            session.run("CREATE INDEX movie_id IF NOT EXISTS FOR (m:Movie) ON (m.id)")
            session.run("CREATE INDEX movie_title IF NOT EXISTS FOR (m:Movie) ON (m.title)")
            session.run("CREATE INDEX movie_year IF NOT EXISTS FOR (m:Movie) ON (m.year)")

            # Try to create constraints (uniqueness)
            try:
                session.run("CREATE CONSTRAINT actor_id_unique IF NOT EXISTS FOR (a:Actor) REQUIRE a.id IS UNIQUE")
                session.run("CREATE CONSTRAINT movie_id_unique IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE")
                print("✓ Indexes and constraints created")
            except Exception as e:
                print(f"✓ Indexes created (constraints not supported: {e})")

    def load_actors(self, csv_path, batch_size=1000):
        """Load actors from CSV in batches"""
        print(f"\nLoading actors from {csv_path}...")
        df = pd.read_csv(csv_path)
        total = len(df)

        start_time = time.time()
        loaded = 0

        with self.driver.session() as session:
            for i in range(0, total, batch_size):
                batch = df.iloc[i:i+batch_size].to_dict('records')

                # Convert numpy types to native Python types
                batch = [
                    {'id': int(row['actorId']), 'name': str(row['name'])}
                    for row in batch
                ]

                session.run("""
                    UNWIND $batch as actor
                    CREATE (a:Actor {id: actor.id, name: actor.name})
                """, batch=batch)

                loaded += len(batch)
                print(f"  Progress: {loaded}/{total} actors ({loaded/total*100:.1f}%)", end='\r')

        elapsed = time.time() - start_time
        rate = total / elapsed

        print(f"\n✓ Loaded {total:,} actors in {elapsed:.2f}s ({rate:.0f} actors/sec)")

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
        loaded = 0

        with self.driver.session() as session:
            for i in range(0, total, batch_size):
                batch = df.iloc[i:i+batch_size].to_dict('records')

                # Convert numpy types to native Python types
                batch = [
                    {'id': int(row['movieId']), 'title': str(row['title']), 'year': int(row['year'])}
                    for row in batch
                ]

                session.run("""
                    UNWIND $batch as movie
                    CREATE (m:Movie {id: movie.id, title: movie.title, year: movie.year})
                """, batch=batch)

                loaded += len(batch)
                print(f"  Progress: {loaded}/{total} movies ({loaded/total*100:.1f}%)", end='\r')

        elapsed = time.time() - start_time
        rate = total / elapsed

        print(f"\n✓ Loaded {total:,} movies in {elapsed:.2f}s ({rate:.0f} movies/sec)")

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
        loaded = 0

        with self.driver.session() as session:
            for i in range(0, total, batch_size):
                batch = df.iloc[i:i+batch_size].to_dict('records')

                # Convert numpy types to native Python types
                batch = [
                    {'actorId': int(row['actorId']), 'movieId': int(row['movieId'])}
                    for row in batch
                ]

                session.run("""
                    UNWIND $batch as role
                    MATCH (a:Actor {id: role.actorId})
                    MATCH (m:Movie {id: role.movieId})
                    CREATE (a)-[:ACTED_IN]->(m)
                """, batch=batch)

                loaded += len(batch)
                print(f"  Progress: {loaded}/{total} relationships ({loaded/total*100:.1f}%)", end='\r')

        elapsed = time.time() - start_time
        rate = total / elapsed

        print(f"\n✓ Loaded {total:,} relationships in {elapsed:.2f}s ({rate:.0f} edges/sec)")

        self.stats['relationships'] = {
            'count': total,
            'time_seconds': elapsed,
            'rate_per_second': rate
        }

    def verify_data(self):
        """Verify loaded data"""
        print("\nVerifying loaded data...")
        with self.driver.session() as session:
            # Count nodes
            result = session.run("MATCH (a:Actor) RETURN count(a) as count")
            actor_count = result.single()["count"]

            result = session.run("MATCH (m:Movie) RETURN count(m) as count")
            movie_count = result.single()["count"]

            # Count relationships
            result = session.run("MATCH ()-[r:ACTED_IN]->() RETURN count(r) as count")
            rel_count = result.single()["count"]

            print(f"  Actors: {actor_count:,}")
            print(f"  Movies: {movie_count:,}")
            print(f"  Relationships: {rel_count:,}")

            # Find Christian Bale
            result = session.run("""
                MATCH (a:Actor {name: 'Christian Bale'})-[:ACTED_IN]->(m:Movie)
                RETURN a.name as actor, count(m) as movie_count
            """)
            record = result.single()
            if record:
                print(f"\n  ✓ Christian Bale found: {record['movie_count']:,} movies")

            self.stats['verification'] = {
                'actors': actor_count,
                'movies': movie_count,
                'relationships': rel_count
            }

    def save_stats(self, output_path):
        """Save loading statistics"""
        self.stats['database'] = 'CognoDB Cloud'
        self.stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')

        # Calculate total time
        total_time = sum(
            self.stats.get(key, {}).get('time_seconds', 0)
            for key in ['actors', 'movies', 'relationships']
        )
        self.stats['total_time_seconds'] = total_time

        with open(output_path, 'w') as f:
            json.dump(self.stats, f, indent=2)

        print(f"\n✓ Statistics saved to {output_path}")

    def close(self):
        """Close database connection"""
        self.driver.close()
        print("\n✓ Connection closed")

def main():
    # Paths
    data_dir = Path("C:/Users/hasann2/Downloads/graph-benchmark/data/processed")
    results_dir = Path("C:/Users/hasann2/Downloads/graph-benchmark/results")
    results_dir.mkdir(exist_ok=True)

    # Initialize loader
    loader = CognoDBLoader()

    # Test connection
    if not loader.test_connection():
        print("Failed to connect. Check your .env file.")
        return

    # Optional: Clear existing data
    # Uncomment if you want to start fresh
    # loader.clear_database()

    # Create indexes
    loader.create_indexes()

    # Load data
    loader.load_actors(data_dir / 'actors.csv')
    loader.load_movies(data_dir / 'movies.csv')
    loader.load_roles(data_dir / 'roles.csv')

    # Verify
    loader.verify_data()

    # Save statistics
    loader.save_stats(results_dir / 'cognodb_loading_stats.json')

    # Close
    loader.close()

    print("\n" + "="*60)
    print("CognoDB data loading complete!")
    print("="*60)

if __name__ == '__main__':
    main()
