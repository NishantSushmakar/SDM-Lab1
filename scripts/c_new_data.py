from neo4j import GraphDatabase
import argparse

def create_parser():
    parser = argparse.ArgumentParser(description="Neo4j connection details")
    parser.add_argument('--uri', type=str, default="bolt://localhost:7687", help="URI for Neo4j connection (default: bolt://localhost:7687)")
    parser.add_argument('--u', type=str, default="neo4j", help="Username for Neo4j connection (default: neo4j)")
    parser.add_argument('--p', type=str, default="", help="Password for Neo4j connection (default: None)")
    parser.add_argument('--db', type=str, default="neo4j", help="Database name for Neo4j connection (default: neo4j)")
    
    return parser

def create_db_community(session):
    session.run("""
        MERGE (db:ResearchCommunity {name: "Database"})

        WITH db, ["data management", "indexing", "data modeling", "big data", 
                "data processing", "data storage", "data querying"] AS keywords
        UNWIND keywords AS keyword
        MERGE (k:Keyword {keyword: keyword})
        MERGE (k)-[:DEFINES]->(db)
    """)
def create_db_venue(session):
    session.run("""
        MATCH (k:Keyword)-[:DEFINES]->(:ResearchCommunity {name: "Database"})
        WITH collect(k) AS dbKeywords

        MATCH (venue)-[:HAS_EDITION|HAS_VOLUME]->(v)<-[:PUBLISHED_IN]-(p:Paper)
        WHERE venue:Journal OR venue:Event
        WITH 
        venue,
        count(DISTINCT p) AS totalPapers,
        count(DISTINCT 
            [(p)-[:RELATED_TO]->(k) WHERE k IN dbKeywords | p][0]
        ) AS relevantPapers
        WHERE relevantPapers/totalPapers > 0.9
        SET venue:DatabaseVenue
    """)

def create_top_edge(session):
    session.run("""
        MATCH (k:Keyword)-[:DEFINES]->(c:ResearchCommunity {name: "Database"})
        WITH collect(k) AS dbKeywords, c

        MATCH (p:Paper)-[:PUBLISHED_IN]->()<-[:HAS_EDITION|HAS_VOLUME]-(v)
        WHERE (v:Journal OR v:Event) AND EXISTS { (p)-[:RELATED_TO]->(k) WHERE k IN dbKeywords }
        WITH collect(DISTINCT p) AS dbPapers, c

        UNWIND dbPapers AS paper
        OPTIONAL MATCH (paper)-[:CITED_IN]->(citingPaper) WHERE citingPaper IN dbPapers
        WITH 
        paper, 
        count(DISTINCT citingPaper) AS dbCitations,
        c
        ORDER BY dbCitations DESC
        WITH 
        collect(paper) AS papersInOrder,
        collect(dbCitations) AS citationCounts,
        c

        UNWIND range(0, size(papersInOrder)-1) AS idx
        WITH 
        papersInOrder[idx] AS paper,
        citationCounts[idx] AS citations,
        idx + 1 AS rank,
        c
        WHERE rank <= 100
        MERGE (paper)-[r:TOP_IN_COMMUNITY]->(c)
        SET r.last_update = date()
    """)

def create_reviewer_label(session):
    session.run("""
        MATCH (p:Paper)-[r:TOP_IN_COMMUNITY]->(:ResearchCommunity {name: "Database"})
        WITH p LIMIT 100000
        MATCH (p)<-[:WROTE]-(a)
        WITH a, count(p) AS topCount
        WHERE topCount >= 1
        SET a:PotentialReviewer
        WITH a WHERE topCount >= 2
        SET a:Guru
    """)

def main():
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
        create_db_venue(session)
        print("Created the database venue label")
        create_top_edge(session)
        print("Created the top in community edge")
        create_reviewer_label(session)
        print("Created the reviewer / guru labels")
    driver.close()

if __name__ == "__main__":
    main()
# Do we need citationCounts[idx] AS citations as property for edge - for updates?
# Consider cases where we will have more than one community