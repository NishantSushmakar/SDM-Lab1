from neo4j import GraphDatabase
import warnings
from helper.argparse import create_parser
import sys
import os

warnings.filterwarnings("ignore")

# Community with property 'name'. Creates 1 node with 7 (DEFINES) relationships 
def create_db_community(session):
    session.run("""
        MERGE (db:Community {name: "Database"})
        WITH db, ["data management", "indexing", "data modeling", "big data", "data processing", "data storage", "data querying"] AS keywords
        UNWIND keywords AS keyword
        MERGE (k:Keyword {keyword: keyword})
        MERGE (k)-[:DEFINES]->(db)
    """)

# VENUE_OF edge. Creates 88 relationships 
def create_venue_of(session):
    session.run("""
        MATCH (k:Keyword)-[:DEFINES]->(c:Community {name: "Database"})
        WITH collect(k) AS dbKeywords, c
        MATCH (venue)-[:HAS_EDITION|HAS_VOLUME]->(ve)<-[:PUBLISHED_IN]-(p:Paper)
        WITH venue, count(DISTINCT p) AS totalPapers,
        count(DISTINCT CASE WHEN EXISTS {(p)-[:RELATED_TO]->(k) WHERE k IN dbKeywords} THEN p END) AS relevantPapers, c
        WHERE totalPapers > 0 AND relevantPapers/totalPapers > 0.9
        MERGE (venue)-[:VENUE_OF]->(c)
    """)

# TOP_IN_COMMUNITY edge with property 'citationCount'. Creates 100 relationships 
def create_top_edge(session):
    session.run("""
        MATCH (p:Paper)-[:PUBLISHED_IN]->(ve)<-[:HAS_EDITION|HAS_VOLUME]-(v)-[:VENUE_OF]->(c:Community {name: "Database"})
        WITH collect(DISTINCT p) AS dbPapers, c
        UNWIND dbPapers AS paper
        OPTIONAL MATCH (paper)-[:CITED_IN]->(citingPaper)
        WHERE citingPaper IN dbPapers
        WITH paper, count(DISTINCT citingPaper) AS dbCitations, c
        ORDER BY dbCitations DESC
        WITH collect(paper) AS papersInOrder, collect(dbCitations) AS citationCounts, c
        UNWIND range(0, size(papersInOrder)-1) AS idx
        WITH papersInOrder[idx] AS paper, citationCounts[idx] AS citations, idx + 1 AS rank, c
        WHERE rank <= 100
        MERGE (paper)-[r:TOP_IN_COMMUNITY]->(c)
        SET r.citationCount = citations
                
    """)

# POTENTIAL_REVIEWER_OF/GURU_OF edges. Creates 325 relationships in total
def create_reviewer_guru_edge(session):
    session.run("""
        MATCH (a:Author)-[:WROTE]->(p:Paper)-[:TOP_IN_COMMUNITY]->(c:Community {name: "Database"})
        WITH a, c, count(p) AS topPaperCount
        FOREACH (ignore IN CASE WHEN topPaperCount >= 1 THEN [1] ELSE [] END |
            MERGE (a)-[:POTENTIAL_REVIEWER_OF]->(c))
        FOREACH (ignore IN CASE WHEN topPaperCount >= 2 THEN [1] ELSE [] END |
            MERGE (a)-[:GURU_OF]->(c))
    """)

def main():
    original_stderr = sys.stderr
    sys.stderr = open(os.devnull, 'w')
    try:
        parser = create_parser()
        args = parser.parse_args()

        # Use the arguments to connect to the Neo4j database
        uri = args.uri
        username = args.u
        password = args.p
        db = args.db
        
        driver = GraphDatabase.driver(uri, auth=(username, password))

        with driver.session(database=db) as session:
            create_db_community(session)
            print("Created the database community label")
            create_venue_of(session)
            print("Created the venue_of community edge")
            create_top_edge(session)
            print("Created the paper top in community edge")
            create_reviewer_guru_edge(session)
            print("Created the author potential_reviewer_of / guru_of community edges")
        driver.close()
        print("\nAll queries completed.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Restore stderr
        sys.stderr = original_stderr

if __name__ == "__main__":
    main()
