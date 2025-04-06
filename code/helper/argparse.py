import argparse

def create_parser():
    parser = argparse.ArgumentParser(description="Neo4j connection details")
    parser.add_argument('--uri', type=str, default="bolt://localhost:7687", help="URI for Neo4j connection (default: bolt://localhost:7687)")
    parser.add_argument('--u', type=str, default="neo4j", help="Username for Neo4j connection (default: neo4j)")
    parser.add_argument('--p', type=str, default="", help="Password for Neo4j connection (default: None)")
    parser.add_argument('--db', type=str, default="neo4j", help="Database name for Neo4j connection (default: neo4j)")
    
    return parser