from semanticscholar import SemanticScholar
from tqdm import tqdm
import json
import os 
import requests

### Selected Keywords for each domain for selecting the papers
domain = {
    "Computer Science":["data management","indexing", "data modeling", "big data", 
          "data processing", "data storage", "data querying"],
    "Medicine":["diagnosis","health","disease","Genomic"],
    "Biology": ["genome","Epigenomic","phylogenetics","receptor"],
    "Chemistry":["synthesis","Bioinorganic","Molecular","Chromatographic"],
    "Mathematics":["curves","probability","Harmonic","hyperbolic"]
}

### Paper Data Fields
fields = ["paperId", "corpusId", "externalIds", "url", "title", "abstract", "venue",
    "publicationVenue", "year", "referenceCount", "citationCount", 
    "influentialCitationCount", "isOpenAccess", "openAccessPdf", 
    "fieldsOfStudy", "s2FieldsOfStudy", "publicationTypes", "publicationDate",
    "journal", "citationStyles", "authors", "citations", "references", "tldr"]

## Author Data Fields

author_fields = ['authorId', 'externalIds', 'url', 'name', 'affiliations', 'homepage', 'paperCount', 'citationCount', 'hIndex']


def main():

    sc = SemanticScholar(timeout=200)
    journal_papers = []
    conference_papers = []

    # ## Fetching Research paper with certain keywords for Journal and conerferences specifically

    for keyword in tqdm(domain.keys()):
            response_journal = sc.search_paper(query="| ".join(domain[keyword]),\
                                               fields=fields,publication_types=["JournalArticle"],\
                                               fields_of_study=[f"{keyword}"],min_citation_count=5)
            response_conference = sc.search_paper(query="| ".join(domain[keyword]),\
                                                  fields=fields,publication_types=["Conference"],\
                                                  fields_of_study=[f"{keyword}"],min_citation_count=5)
            journal_papers.extend(response_journal.items)
            conference_papers.extend(response_conference.items)
    
    journal_papers = journal_papers[:5]
    conference_papers = conference_papers[:5]

    new_conference_papers = [dict(x) for x in conference_papers]
    new_journal_papers = [dict(x) for x in journal_papers]

    if not os.path.exists("./paper_data"):
         os.mkdir("./paper_data")

    with open("./paper_data/conference_papers.json","w") as f:
        json.dump(new_conference_papers,f)

    with open("./paper_data/journal_papers.json","w") as f:
        json.dump(new_journal_papers,f)

    print("Conference and Journal Research Papers Fetched Successfully!!")

    ## Collecting the citation list for fetched papers
    lst_of_paperids_conference = []
    for x in conference_papers:
        lst_of_paperids_conference.extend([cite['paperId'] for cite in x['citations'] if cite['paperId'] != None])   

    lst_of_paperids_conference = list(set(lst_of_paperids_conference))

    lst_of_paperids_journal = []
    for x in journal_papers:
        lst_of_paperids_journal.extend([cite['paperId'] for cite in x['citations'] if cite['paperId'] != None])  

    lst_of_paperids_journal = list(set(lst_of_paperids_journal)) 

    ## Fetching Paper Details for Citation paperids in a batch size 500
    chunk_size = 500 
    cited_conference_papers = []

    for i in tqdm(range(0, len(lst_of_paperids_conference), chunk_size)):
        
        chunk = lst_of_paperids_conference[i:min(i + chunk_size,len(lst_of_paperids_conference))]
        response = sc.get_papers(paper_ids=chunk,fields=fields)
        cited_conference_papers.extend(response)

    chunk_size = 500 
    cited_journal_papers = []

    new_cited_conference_papers = [dict(x) for x in cited_conference_papers]

    for i in tqdm(range(0, len(lst_of_paperids_journal), chunk_size)):
        
        chunk = lst_of_paperids_journal[i:min(i + chunk_size,len(lst_of_paperids_journal))]
        response = sc.get_papers(paper_ids=chunk,fields=fields)
        cited_journal_papers.extend(response)

    new_cited_journal_papers = [dict(x) for x in cited_journal_papers]

    with open("./paper_data/conference_papers_citations.json","w") as f:
        json.dump(new_cited_conference_papers,f)

    with open("./paper_data/journal_papers_citations.json","w") as f:
        json.dump(new_cited_journal_papers,f)

    print("Cited Conference and Journal Papers fetched successfully!!!")

    ## Collecting all the authorids from fetched papers
    lst_of_author_ids = []

    for paper in conference_papers :
        lst_of_author_ids.extend([author['authorId'] for author in paper['authors']])

    for paper in journal_papers :
        lst_of_author_ids.extend([author['authorId'] for author in paper['authors']])

    for paper in cited_conference_papers :
        lst_of_author_ids.extend([author['authorId'] for author in paper['authors']])

    for paper in cited_journal_papers :
        lst_of_author_ids.extend([author['authorId'] for author in paper['authors']])

    
    lst_of_author_ids = list(set(lst_of_author_ids))

    ## Fetching Author details in batch size of 100
    chunk_size = 100
    author_details = []

    for i in tqdm(range(0, len(lst_of_author_ids), chunk_size)):
        
        chunk = lst_of_author_ids[i:min(i + chunk_size,len(lst_of_author_ids))]
        response = sc.get_authors(chunk,fields=author_fields)

        author_details.extend(response)

    new_author_details = [dict(x) for x in author_details]

    with open("./paper_data/authors_details_new.json","w") as f:
        json.dump(new_author_details,f)

    print("Author Details Fetched Successfully!!!")

    combined_papers_data = []
    seen_ids = set()

    # Loop through each JSON file in the directory
    json_dir = './paper_data'

    for filename in os.listdir(json_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(json_dir, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)  # Load JSON data
                # Filter data
                filtered_data = [
                    paper for paper in data
                    if paper is not None and paper.get("publicationTypes") is not None and ("JournalArticle" in paper.get("publicationTypes", [])
                        or "Conference" in paper.get("publicationTypes", []))
                ]
                for paper in filtered_data:
                    paper_id = paper.get("paperId")
                    journal = paper.get("journal", [])
                    if "JournalArticle" in paper.get("publicationTypes", []) and journal is None and "Conference" not in paper.get("publicationTypes", []):
                        continue
                    if "JournalArticle" in paper.get("publicationTypes", []) and "Conference" not in paper.get("publicationTypes", []) and (journal.get("volume") is None or journal.get("volume")==""):
                        continue
                    if paper_id and paper_id not in seen_ids:
                        seen_ids.add(paper_id)
                        combined_papers_data.append(paper)

    # Write combined data to a new JSON file
    with open('./combined_papers_data.json', 'w') as outfile:
        json.dump(combined_papers_data, outfile, indent=4)

    print("JSON files combined successfully!")
    
    paper_lst = []
    paths = ['combined_papers_data.json']
    for path in paths:
        with open(f'./{path}','r') as f:
            data = json.load(f)

        paper_lst.extend(data)
    
    print("Paper Loaded Successfully")

    alex_info_lst = []

    for paper in tqdm(paper_lst):
        try:  
            if len(paper['externalIds']) > 0:
                if 'DOI' in paper['externalIds']:
                    doi = paper['externalIds']['DOI']
                    doi_url = f"https://api.openalex.org/works/https://doi.org/{doi}"
                    response_url = requests.get(doi_url).json()
                    response_url['paperId'] = paper['paperId']
                    alex_info_lst.append(response_url)   
                elif 'MAG' in paper['externalIds']:
                    mag = paper['externalIds']['MAG']
                    mag_url = f"https://api.openalex.org/works?filter=ids.mag:{mag}"
                    response_url = requests.get(mag_url).json()
                    response_url['paperId'] = paper['paperId']
                    alex_info_lst.append(response_url)  

        except Exception as e:
            print(f'error as {e}')

    with open('./paper_data/alex_info.json','w') as f:
        json.dump(alex_info_lst,f)

    print("Keywords Data Fetched Successfully!!!")


if __name__ == "__main__":

    main()
  
