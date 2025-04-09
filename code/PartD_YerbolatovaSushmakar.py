from neo4j import GraphDatabase
from helper.argparse import create_parser
from helper.output import execute_query_with_output
import sys
import warnings
import os

warnings.filterwarnings("ignore")


def page_rank_query(session,output_file):

    try:
        session.run("""
            CALL gds.graph.project(
            'citationgraph',
            'Paper',
            { CITED_IN: { orientation: 'REVERSE' } }
            );
        """)
    except Exception:
  
        pass

    query = """
        CALL gds.pageRank.stream('citationgraph', { maxIterations: 50 })
        YIELD nodeId, score
        WITH gds.util.asNode(nodeId) AS paper, score
        RETURN paper.title AS influentialPaper, score
        ORDER BY score DESC;
        
        """
    
    execute_query_with_output(session, query, output_file, "Algorithm 1- Page Rank")


def node_similarity_query(session,output_file):

    try:
        session.run("""
            CALL gds.graph.project(
            'paperKeywordNetwork',
            ['Paper', 'Keyword'],
            'RELATED_TO'
            );

        """)
    except Exception:  
        pass

    query = """
        CALL gds.nodeSimilarity.stream('paperKeywordNetwork')
        YIELD node1, node2, similarity
        MATCH (p1:Paper), (p2:Paper) WHERE id(p1) = node1 AND id(p2) = node2
        RETURN p1.title AS paperA, p2.title AS paperB, similarity
        ORDER BY similarity DESC;
        """
    
    execute_query_with_output(session, query, output_file, "Algorithm 2- Node Similarity")


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
        
        # Output file path - can be modified or made configurable
        output_file_path = "neo4j_graph_algo_query_results.txt"
        
        print(f"Connecting to Neo4j database at {uri}...")
        driver = GraphDatabase.driver(uri, auth=(username, password))

        with open(output_file_path, 'w') as output_file:
            output_file.write("NEO4J GRAPH ALGORITHMS QUERY RESULTS\n")
            output_file.write("===================\n")
            
            with driver.session(database=db) as session:
                page_rank_query(session, output_file)
                print("Algorithm 1 executed - results written to file")
                
                node_similarity_query(session, output_file)
                print("Algorithm 2 executed - results written to file")
        
        driver.close()
        print(f"\nAll queries completed. Results saved to {output_file_path}")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Restore stderr
        sys.stderr = original_stderr


if __name__ == "__main__":

    main()