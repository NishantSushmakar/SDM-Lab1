def create_constraints(tx):
    # Create constraints to ensure uniqueness
    tx.run("CREATE CONSTRAINT affiliation_id IF NOT EXISTS FOR (af:Affiliation) REQUIRE af.affId IS UNIQUE")

def load_nodes(session):
    # Load nodes
    # Affiliation nodes
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///affiliation.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            CREATE (af:Affiliation {affId: row.affId, name: row.name})
        } IN TRANSACTIONS OF 200 ROWS
    """)

def load_edges(session):
    # Load edges
    # author_affiliatedWith edge
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_affiliatedWith_affiliation.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (a:Author {authorId: toInteger(row.authorId)})
            MATCH (af:Affiliation {affId: row.affId})
            CREATE (a)-[:AFFILIATED_WITH]->(af)
        } IN TRANSACTIONS OF 200 ROWS
    """)
    # Reviewed edge with new properties
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///review_relations.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            MATCH (a:Author {authorId: toInteger(row.authorId)})-[rel:REVIEWED]->(p:Paper {paperId: row.paperId})
            SET rel.comments = row.comments,
                rel.vote = row.vote
        } IN TRANSACTIONS OF 200 ROWS
    """)

def ingestion(driver):
    with driver.session() as session:
        # Create constraints
        session.execute_write(create_constraints)
        print("Constraints created")
        # Load data
        load_nodes(session)
        print("Nodes loaded successfully")
        load_edges(session)
        print("Edges loaded successfully")
    driver.close()