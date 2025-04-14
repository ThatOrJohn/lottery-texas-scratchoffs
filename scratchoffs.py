from neo4j import GraphDatabase
import os

# Neo4j AuraDB credentials (store in environment variables for security)
AURA_URI = os.getenv("NEO4J_URI", "neo4j+s://<instance_id>.databases.neo4j.io")
AURA_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
AURA_PASSWORD = os.getenv("NEO4J_PASSWORD", "geTyOUrOWn")

# URL of the publicly accessible CSV file
CSV_URL = os.getenv(
    "CSV_URL", "https://www.texaslottery.com/export/sites/lottery/Games/Scratch_Offs/scratchoff.csv")


def update_neo4j():
    driver = GraphDatabase.driver(
        AURA_URI, auth=(AURA_USERNAME, AURA_PASSWORD))
    with driver.session() as session:
        # Extract the date from the first row
        date_query = """
        LOAD CSV FROM $csv_url AS row
        LIMIT 1
        RETURN split(row[0], ' as of ')[1] AS date_updated
        """
        date_result = session.run(date_query, csv_url=CSV_URL)
        date_updated = date_result.single()['date_updated']
        print(f"Extracted date: {date_updated}")

        # Debug 1: Inspect raw rows (no headers assumed)
        debug_raw_query = """
        LOAD CSV FROM $csv_url AS row
        RETURN row
        LIMIT 5
        """
        debug_raw_result = session.run(debug_raw_query, csv_url=CSV_URL)
        print("Debugging raw CSV rows (no headers assumed):")
        for record in debug_raw_result:
            print(f"Row: {record['row']}")

        # Debug 2: Count total rows (before skipping)
        total_rows_query = """
        LOAD CSV FROM $csv_url AS row
        RETURN count(*) AS total_rows
        """
        total_rows_result = session.run(total_rows_query, csv_url=CSV_URL)
        total_rows = total_rows_result.single()['total_rows']
        print(f"Total rows in CSV: {total_rows}")

        # Debug 3: Inspect data rows (after skipping metadata and header rows)
        debug_data_query = """
        LOAD CSV FROM $csv_url AS row
        SKIP 1
        WITH row
        SKIP 1
        RETURN row
        LIMIT 5
        """
        debug_data_result = session.run(debug_data_query, csv_url=CSV_URL)
        print("Debugging data rows (after skipping metadata and header):")
        for record in debug_data_result:
            print(f"Row: {record['row']}")

        # Clear existing data (optional, depending on your use case)
        session.run("MATCH (n) DETACH DELETE n")

        # Step 1: Import Game nodes (Prize Level = 'TOTAL')
        query_game = """
        LOAD CSV FROM $csv_url AS row
        SKIP 1
        WITH row
        SKIP 1
        WITH row
        WHERE trim(replace(row[4], '"', '')) = 'TOTAL'
        AND row[0] IS NOT NULL
        MERGE (g:Game {game_number: row[0]})
        SET g.game_name = row[1],
            g.game_close_date = row[2],
            g.ticket_price = toFloat(row[3]),
            g.total_prizes = toInteger(row[5]),
            g.prizes_claimed = toInteger(row[6]),
            g.date_updated = $date_updated
        RETURN count(*) AS total_games
        """
        result_game = session.run(
            query_game, csv_url=CSV_URL, date_updated=date_updated)
        print(f"Imported {result_game.single()['total_games']} Game nodes")

        # Step 2: Import Detail nodes and create relationships (Prize Level <> 'TOTAL')
        query_detail = """
        LOAD CSV FROM $csv_url AS row
        SKIP 1
        WITH row
        SKIP 1
        WITH row
        WHERE trim(replace(row[4], '"', '')) <> 'TOTAL'
        AND row[0] IS NOT NULL
        MERGE (d:Detail {game_number: row[0], prize_level: row[4]})
        SET d.total_prizes = toInteger(row[5]),
            d.prizes_claimed = toInteger(row[6])
        WITH d, row
        MATCH (g:Game {game_number: row[0]})
        MERGE (d)-[:BELONGS_TO]->(g)
        RETURN count(*) AS total_details
        """
        result_detail = session.run(query_detail, csv_url=CSV_URL)
        print(
            f"Imported {result_detail.single()['total_details']} Detail nodes")

    driver.close()


if __name__ == "__main__":
    try:
        update_neo4j()
    except Exception as e:
        print(f"Error during import: {e}")
