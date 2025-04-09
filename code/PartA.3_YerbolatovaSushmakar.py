from helper import argparse,\
                   a3_create_additional_data, \
                   a3_ingest

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

    a3_create_additional_data.create_add_data(data_path,output_path)
    a3_ingest.ingestion(driver)


    





