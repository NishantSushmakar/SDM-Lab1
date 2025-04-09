from neo4j import GraphDatabase
from helper.argparse import create_parser
from helper.output import execute_query_with_output
import sys
import warnings
import os

warnings.filterwarnings("ignore")


def query_1(session, output_file):
    query = """
        MATCH (ed:Edition)<-[:HAS_EDITION]-(c)
        MATCH (ed)<-[:PUBLISHED_IN]-(cited)
        OPTIONAL MATCH (cited)-[:CITED_IN]->(p)  
        WITH c, cited, COUNT(p) AS citations     	 
        ORDER BY c.name, citations DESC          	 
        WITH c, COLLECT({paper: cited, citations: citations})[0..3] AS top_papers
        UNWIND top_papers AS entry
        RETURN c.name AS Event, entry.paper.title AS PaperTitle, entry.citations AS Citations
        ORDER BY Event, Citations DESC;
    """
    execute_query_with_output(session, query, output_file, "Query 1: Find the top 3 most cited papers of each conference/workshop")
    
    
def query_2(session, output_file):
    query = """
        MATCH (c:Event)-[:HAS_EDITION]->(ed)
        MATCH (ed)<-[:PUBLISHED_IN]-(p)
        MATCH (p)<-[:WROTE]-(a)
        WITH c, a, COUNT(DISTINCT ed) AS distinct_edition_participated
        WHERE distinct_edition_participated >= 4
        RETURN
        c.name AS Event,
        a.name AS Author,
        distinct_edition_participated AS EditionsParticipated
        ORDER BY Event, EditionsParticipated DESC;
    """
    execute_query_with_output(session, query, output_file, \
    "Query 2: For each conference/workshop find its community: i.e., those authors that have" \
    "published papers on that conference/workshop in, at least, 4 different editions.")


def query_3(session, output_file):
    # Create index if it doesn't exist
    try:
        session.run("""
            CREATE INDEX volume_year IF NOT EXISTS FOR (v:Volume) ON v.year;
        """)
    except Exception:
       
        pass
    
    query = """
        WITH 2017 AS targetYear
        MATCH (v:Volume)
        WHERE v.year IN [targetYear - 1, targetYear - 2]
        MATCH (j)-[:HAS_VOLUME]->(v)
        MATCH (v)<-[:PUBLISHED_IN]-(p)
        OPTIONAL MATCH (p)<-[:CITED_IN]-(citingPaper)-[:PUBLISHED_IN]->(citingVolume)
        WHERE citingVolume.year = targetYear
        WITH j,
            COUNT(DISTINCT p) AS totalPapers,
            COUNT(citingPaper) AS totalCitations
        RETURN
        j.name AS Journal,
        CASE
            WHEN totalPapers > 0 THEN totalCitations / toFloat(totalPapers)
            ELSE 0.0
        END AS ImpactFactor 
        ORDER BY ImpactFactor DESC;
    """
    execute_query_with_output(session, query, output_file, \
    "Query 3:  Find the impact factor of the journals in your graph (for the query we have taken the target year 2017)")


def query_4(session, output_file):
    query = """
        MATCH (a:Author)-[:WROTE]->(p)
        OPTIONAL MATCH (p)-[:CITED_IN]->(citingPaper)
        WITH a, p, COUNT(citingPaper) AS citationCount
        ORDER BY citationCount DESC
        WITH a, COLLECT({paper: p, citations: citationCount}) AS papers
        WITH a, REDUCE(s = 0, x IN RANGE(0, SIZE(papers)-1) |
        CASE WHEN papers[x].citations >= x + 1 THEN s + 1 ELSE s END) AS hIndex
        RETURN a.name AS Author, hIndex AS HIndex 
        ORDER BY HIndex DESC;
    """
    execute_query_with_output(session, query, output_file, "Query 4: Find the h-index of the authors in your graph.")


def main():
    # Redirect stderr to suppress warnings
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
        
        # Output file path - can be modified or made configurable
        output_file_path = "neo4j_query_results.txt"
        
        print(f"Connecting to Neo4j database at {uri}...")
        driver = GraphDatabase.driver(uri, auth=(username, password))

        with open(output_file_path, 'w') as output_file:
            output_file.write("NEO4J QUERY RESULTS\n")
            output_file.write("===================\n")
            
            with driver.session(database=db) as session:
                query_1(session, output_file)
                print("Query 1 executed - results written to file")
                
                query_2(session, output_file)
                print("Query 2 executed - results written to file")
                
                query_3(session, output_file)
                print("Query 3 executed - results written to file")
                
                query_4(session, output_file)
                print("Query 4 executed - results written to file")
        
        driver.close()
        print(f"\nAll queries completed. Results saved to {output_file_path}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Restore stderr
        sys.stderr = original_stderr


if __name__ == "__main__":
    main()