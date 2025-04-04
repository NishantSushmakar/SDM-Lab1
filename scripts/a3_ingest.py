from neo4j import GraphDatabase

# Neo4j connection details
URI = "bolt://localhost:7687"
USERNAME = "neo4j"
PASSWORD = "DunderMifflin"

driver = GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD))

def create_constraints(tx):
    # Create constraints to ensure uniqueness
    tx.run("CREATE CONSTRAINT affiliation_id IF NOT EXISTS FOR (af:Affiliation) REQUIRE af.affId IS UNIQUE")
    tx.run("CREATE CONSTRAINT review_id IF NOT EXISTS FOR (r:Review) REQUIRE r.reviewId IS UNIQUE")

def load_nodes(session):
    # Load nodes
    # Affiliation nodes
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///affiliation.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            CREATE (af:Affiliation {affId: row.affId, name: row.name})
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # # Review nodes
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///review.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            CREATE (r:Review {reviewId: row.reviewId, comments: row.comments, vote: row.vote})
        } IN TRANSACTIONS OF 200 ROWS
    """)

def load_edges(session):
    # Load edges
    session.run("""
    MATCH (a:Author)-[rel:REVIEWED]->(p:Paper)
    DELETE rel """)
    # author_affiliatedWith edge
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_affiliatedWith_affiliation.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (a:Author {authorId: toInteger(row.authorId)})
            MATCH (af:Affiliation {affId: row.affId})
            CREATE (a)-[:AFFILIATED_WITH]->(af)
        } IN TRANSACTIONS OF 200 ROWS
    """)
    # Review edges
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///review_relations.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            MATCH (r:Review {reviewId: row.reviewId})
            MATCH (p:Paper {paperId: row.paperId})
            MATCH (a:Author {authorId: toInteger(row.authorId)})
            CREATE (a)-[:WROTE_REVIEW]->(r)-[:REVIEW_OF]->(p)
        } IN TRANSACTIONS OF 200 ROWS
    """)

def main():
    with driver.session() as session:
        # Create constraints
        session.execute_write(create_constraints)
        print("Constraints created")
        load_nodes(session)
        print("Nodes loaded successfully")
        load_edges(session)
        print("Edges loaded successfully")
    driver.close()

if __name__ == "__main__":
    main()


# // Delete all nodes and relationships
# MATCH (n)
# DETACH DELETE n;

# // Delete constraints
# DROP CONSTRAINT event_id IF EXISTS;
# DROP CONSTRAINT keyword_id IF EXISTS;
# DROP CONSTRAINT paper_id IF EXISTS;
# DROP CONSTRAINT edition_id IF EXISTS;
# DROP CONSTRAINT author_id IF EXISTS;
# DROP CONSTRAINT journal_id IF EXISTS;
# DROP CONSTRAINT volume_id IF EXISTS;
# DROP CONSTRAINT affiliation_id IF EXISTS;
# DROP CONSTRAINT review_id IF EXISTS;

# // Delete indexes (if any)
# DROP INDEX paper_title_index IF EXISTS;



# def load_data_with_load_csv(tx):
#     # Create constraints and indexes for faster processing
#     constraints = [
#         "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Event) REQUIRE e.eventId IS UNIQUE",
#         "CREATE CONSTRAINT IF NOT EXISTS FOR (k:Keyword) REQUIRE k.keywordId IS UNIQUE",
#         "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Paper) REQUIRE p.paperId IS UNIQUE",
#         "CREATE CONSTRAINT IF NOT EXISTS FOR (ed:Edition) REQUIRE ed.editionId IS UNIQUE",
#         "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Author) REQUIRE a.authorId IS UNIQUE",
#         "CREATE CONSTRAINT IF NOT EXISTS FOR (j:Journal) REQUIRE j.journalId IS UNIQUE",
#         "CREATE CONSTRAINT IF NOT EXISTS FOR (v:Volume) REQUIRE v.volumeId IS UNIQUE"
#     ]
#     for constraint in constraints:
#         tx.run(constraint)

#     print("Loading nodes...")
#     # Load nodes
#     node_queries = [
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/event.csv' AS row
#         CREATE (e:Event {{eventId: row.eventId, name: row.name, ISSN: row.ISSN, url: row.url}})
#         SET e:{'{'}row.type{'}'}
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/keyword.csv' AS row
#         CREATE (:Keyword {{keywordId: row.keywordId, keyword: row.keyword}})
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/paper.csv' AS row
#         CREATE (:Paper {{paperId: row.paperId, url: row.url, title: row.title, abstract: row.abstract}})
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/edition.csv' AS row
#         CREATE (:Edition {{editionId: row.editionId, edition: row.edition, location: row.location, year: toInteger(toInteger(row.year))}})
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/author.csv' AS row
#         CREATE (:Author {{authorId: row.authorId, name: row.name}})
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/journal.csv' AS row
#         CREATE (:Journal {{journalId: row.journalId, name: row.name, ISSN: row.ISSN, url: row.url}})
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/volume.csv' AS row
#         CREATE (:Volume {{volumeId: row.volumeId, number: toInteger(row.number), year: toInteger(toInteger(row.year))}})
#         """
#     ]
#     for query in node_queries:
#         tx.run(query)

#     print("Loading relationships...")
#     # Load relationships
#     relationship_queries = [
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/paper_correspondedBy_author.csv' AS row
#         MATCH (p:Paper {{paperId: row.paperId}}), (a:Author {{authorId: row.authorId}})
#         CREATE (p)-[:CORRESPONDED_BY]->(a)
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/author_reviewed_paper.csv' AS row
#         MATCH (a:Author {{authorId: row.authorId}}), (p:Paper {{paperId: row.paperId}})
#         CREATE (a)-[:REVIEWED]->(p)
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/paper_citedIn_paper.csv' AS row
#         MATCH (p1:Paper {{paperId: row.paperId}}), (p2:Paper {{paperId: row.citingPaperId}})
#         CREATE (p1)-[:CITED_IN]->(p2)
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/paper_publishedIn_edition.csv' AS row
#         MATCH (p:Paper {{paperId: row.paperId}}), (ed:Edition {{editionId: row.editionId}})
#         CREATE (p)-[:PUBLISHED_IN {{pages: row.pages}}]->(ed)
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/paper_isRelatedTo_keyword.csv' AS row
#         MATCH (p:Paper {{paperId: row.paperId}}), (k:Keyword {{keywordId: row.keywordId}})
#         CREATE (p)-[:IS_RELATED_TO]->(k)
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/event_hasEdition_edition.csv' AS row
#         MATCH (e:Event {{eventId: row.eventId}}), (ed:Edition {{editionId: row.editionId}})
#         CREATE (e)-[:HAS_EDITION]->(ed)
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/paper_publishedIn_volume.csv' AS row
#         MATCH (p:Paper {{paperId: row.paperId}}), (v:Volume {{volumeId: row.volumeId}})
#         CREATE (p)-[:PUBLISHED_IN {{pages: toInteger(row.pages)}}]->(v)
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/journal_hasVolume_volume.csv' AS row
#         MATCH (j:Journal {{journalId: row.journalId}}), (v:Volume {{volumeId: row.volumeId}})
#         CREATE (j)-[:HAS_VOLUME]->(v)
#         """,
#         f"""
#         LOAD CSV WITH HEADERS FROM '{CSV_DIR}/author_wrote_paper.csv' AS row
#         MATCH (a:Author {{authorId: row.authorId}}), (p:Paper {{paperId: row.paperId}})
#         CREATE (a)-[:WROTE]->(p)
#         """
#     ]
#     for query in relationship_queries:
#         tx.run(query)

# # Main execution
# with GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD)) as driver:
#     with driver.session() as session:
#         session.execute_write(load_data_with_load_csv)

# print("Data loading completed.")
