def create_constraints(tx):
    # Create constraints to ensure uniqueness
    tx.run("CREATE CONSTRAINT event_id IF NOT EXISTS FOR (e:Event) REQUIRE e.eventId IS UNIQUE")
    tx.run("CREATE CONSTRAINT keyword_id IF NOT EXISTS FOR (k:Keyword) REQUIRE k.keywordId IS UNIQUE")
    tx.run("CREATE CONSTRAINT paper_id IF NOT EXISTS FOR (p:Paper) REQUIRE p.paperId IS UNIQUE")
    tx.run("CREATE CONSTRAINT edition_id IF NOT EXISTS FOR (ed:Edition) REQUIRE ed.editionId IS UNIQUE")
    tx.run("CREATE CONSTRAINT author_id IF NOT EXISTS FOR (a:Author) REQUIRE a.authorId IS UNIQUE")
    tx.run("CREATE CONSTRAINT journal_id IF NOT EXISTS FOR (j:Journal) REQUIRE j.journalId IS UNIQUE")
    tx.run("CREATE CONSTRAINT volume_id IF NOT EXISTS FOR (v:Volume) REQUIRE v.volumeId IS UNIQUE")

def load_nodes(session):
    # Load nodes
    # Event nodes (multilabeled)
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///event.csv' AS row FIELDTERMINATOR ','
            CREATE (e:Event:$(row.type) {eventId: row.eventId, name: row.name, ISSN: row.ISSN, url: row.url})
            RETURN labels(e) AS labels
    """)

    # Keyword nodes
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///keyword.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            CREATE (k:Keyword {keywordId: row.keywordId, keyword: row.keyword})
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # Paper nodes
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            CREATE (p:Paper {paperId: row.paperId, url: row.url, title: row.title, abstract: row.abstract})
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # Edition nodes
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///edition.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            CREATE (ed:Edition {editionId: row.editionId, edition: toInteger(row.edition), location: row.location, year: toInteger(row.year)})
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # Author nodes (authorId is integer)
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            CREATE (a:Author {authorId: toInteger(row.authorId), name: row.name})
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # Journal nodes
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///journal.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            CREATE (j:Journal {journalId: row.journalId, name: row.name, ISSN: row.ISSN, url: row.url})
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # Volume nodes
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///volume.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            CREATE (v:Volume {volumeId: row.volumeId, number: row.number, year: toInteger(row.year)})
        } IN TRANSACTIONS OF 200 ROWS
    """)
def load_edges(session):
    # Load edges
    # paper_correspondedBy_author
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_correspondedBy_author.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (p:Paper {paperId: row.paperId})
            MATCH (a:Author {authorId: toInteger(row.authorId)})
            CREATE (p)-[:CORRESPONDED_BY]->(a)
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # author_reviewed_paper
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_reviewed_paper.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (a:Author {authorId: toInteger(row.authorId)})
            MATCH (p:Paper {paperId: row.paperId})
            CREATE (a)-[:REVIEWED]->(p)
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # paper_citedIn_paper
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_citedIn_paper.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (p1:Paper {paperId: row.paperId})
            MATCH (p2:Paper {paperId: row.citingPaperId})
            CREATE (p1)-[:CITED_IN]->(p2)
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # paper_publishedIn_edition
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_publishedIn_edition.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (p:Paper {paperId: row.paperId})
            MATCH (ed:Edition {editionId: row.editionId})
            CREATE (p)-[:PUBLISHED_IN {pages: row.pages}]->(ed)
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # paper_isRelatedTo_keyword
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_isRelatedTo_keyword.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (p:Paper {paperId: row.paperId})
            MATCH (k:Keyword {keywordId: row.keywordId})
            CREATE (p)-[:RELATED_TO]->(k)
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # event_hasEdition_edition
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///event_hasEdition_edition.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (e:Event {eventId: row.eventId})
            MATCH (ed:Edition {editionId: row.editionId})
            CREATE (e)-[:HAS_EDITION]->(ed)
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # paper_publishedIn_volume
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///paper_publishedIn_volume.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (p:Paper {paperId: row.paperId})
            MATCH (v:Volume {volumeId: row.volumeId})
            CREATE (p)-[:PUBLISHED_IN {pages: row.pages}]->(v)
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # journal_hasVolume_volume
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///journal_hasVolume_volume.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (j:Journal {journalId: row.journalId})
            MATCH (v:Volume {volumeId: row.volumeId})
            CREATE (j)-[:HAS_VOLUME]->(v)
        } IN TRANSACTIONS OF 200 ROWS
    """)

    # author_wrote_paper
    session.run("""
        LOAD CSV WITH HEADERS FROM 'file:///author_wrote_paper.csv' AS row FIELDTERMINATOR ','
        CALL (row) {
            
            MATCH (a:Author {authorId: toInteger(row.authorId)})
            MATCH (p:Paper {paperId: row.paperId})
            CREATE (a)-[:WROTE]->(p)
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


