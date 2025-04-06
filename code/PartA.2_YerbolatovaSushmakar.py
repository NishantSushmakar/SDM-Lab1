from helper import argparse,\
                   a2_preprocessing,\
                   a2_create_nodes, \
                   a2_create_edges,\
                   a2_ingest

from neo4j import GraphDatabase



if __name__ == "__main__":

    parser = argparse.create_parser()
    args = parser.parse_args()

    # Use the arguments to connect to the Neo4j database
    uri = args.uri
    username = args.u
    password = args.p
    db = args.db
    data_path = args.data_path
    output_path = args.output_path


    driver = GraphDatabase.driver(uri, auth=(username, password))

    a2_preprocessing.preprocessing()
    a2_create_nodes.create_nodes(data_path,output_path)
    a2_create_edges.create_edges(data_path,output_path)
    a2_ingest.ingestion(driver)


    





