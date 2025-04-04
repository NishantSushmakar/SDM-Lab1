
import argparse
import csv
import os
import json
import pandas as pd

def load_json(json_file_path):
    if os.path.exists(json_file_path):
        try:
            with open(json_file_path, 'r') as f:
                all_elements = json.load(f)
                print(f"Loaded {len(all_elements)} elements from {json_file_path}")
                return all_elements
        except json.JSONDecodeError:
            print(f"Error loading JSON from {json_file_path}, starting with empty list")
            
def load_keyword_to_id(keywords_node):
    keyword_to_id = {}
    with open(keywords_node, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            keyword_to_id[row['keyword']] = row['keywordId']
    return keyword_to_id

def load_volume_to_id(volume_file):
    volume_to_id = {}
    with open(volume_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            volume_key = (row['journalId'], row['year'], row['number'])
            volume_to_id[volume_key] = row['volumeId']
    return volume_to_id

def create_author_wrote_edge(output,all_papers):
    count = 0

    author_paper_pairs = set()
    with open(output, 'w', newline='') as csvfile:
        columns = [
                "authorId","paperId"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        author_count=0
        for paper in all_papers:
            
            if paper is None:  # Skip None entries
                continue
            author_count+=len(paper['authors'])
            authors = paper.get('authors', None)
            paperId=paper.get('paperId', None)
            for author in authors:
                if author is None:
                    continue
                authorId=author.get('authorId', None)
                if (authorId, paperId) not in author_paper_pairs and authorId:
                    writer.writerow({
                        'authorId': authorId,
                        'paperId': paperId
                    })
                    author_paper_pairs.add((authorId, paperId))
                    count += 1

        print(f'Added {count} author-wrote-paper relations from total {author_count} to {output}')

def create_author_reviewed_edge(output,paper_reviews):
    count = 0

    with open(output, 'w', newline='') as csvfile:
        columns = [
                "authorId","paperId"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        result = set()
        for paperId, authorIds in paper_reviews.items():
            for authorId in authorIds:
                result.add((authorId,paperId))
                count+=1
        writer.writerows({"authorId": authorId, "paperId": paperId} for authorId, paperId, in result)
        print(f'Added {count} author_reviewed relations to {output}')

def create_paper_correspondedBy_edge(output,all_papers):
    count = 0

    with open(output, 'w', newline='') as csvfile:
        columns = [
                "paperId", "authorId"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for paper in all_papers:
            
            if paper is None:  # Skip None entries
                continue
            corr_author = paper.get('authors', None)
            if corr_author is None:
                continue
            if corr_author and corr_author[0]['authorId']:
                writer.writerow({
                    'paperId': paper.get('paperId', None),
                    'authorId': corr_author[0]['authorId']
                })
                count += 1

        print(f'Added {count} paper_corresponded_by relations to {output}')

def create_paper_isRelatedTo_edge(output, paper_keywords, keyword_to_id):
    count = 0

    with open(output, 'w', newline='') as csvfile:
        columns = [
                "paperId", "keywordId"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for paper_id, keywords in paper_keywords.items():
            for keyword in keywords:
                lower_key=keyword.lower()
                keyword_id = keyword_to_id[lower_key]
                writer.writerow({
                    'paperId': paper_id,
                    'keywordId': keyword_id
                })
                count+=1
        print(f'Added {count} paper_isRelatedTo relations to {output}')

def create_paper_publishedIn_ed_edge(output, paper_event_ed):
    paper_edition_df = paper_event_ed[['paperId', 'editionId', 'pages']].drop_duplicates()

    paper_edition_df.to_csv(output, index=False)
    print(f'Added {len(paper_edition_df)} paper_publishedIn relations to {output} for events')
    
def create_paper_publishedIn_vol_edge(output, all_papers, volume_to_id):
    count = 0

    with open(output, 'w', newline='') as csvfile:
        columns = [
                "paperId", "volumeId", "pages"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for paper in all_papers:
            
            if paper is None:  # Skip None entries
                continue
            year=str(paper.get('year', None)).strip()
            venue=paper.get('publicationVenue') 
            if ('JournalArticle' in paper.get('publicationTypes', None)
            and 'Conference' not in paper.get('publicationTypes', None) and venue is not None):
                paperId=paper.get('paperId', None)
                journal = paper.get('journal', None)
                number=str(journal.get('volume',None)).strip()
                journal_id = str(venue.get('id')).strip()
                if not journal_id or not year or not number:
                    print(f"Skipping paper due to missing data: paperId={paperId}, journal_id={journal_id}, year={year}, number={number}")
                    continue

                volume_key = (journal_id, year, number)
                # print(f"Volume Key Searched: {volume_key}")

                if volume_key in volume_to_id:
                    volumeId = volume_to_id[volume_key]
                    writer.writerow({
                        'paperId': paperId,
                        'volumeId': volumeId,
                        'pages': journal.get('pages')
                    })
                    count += 1
                else:
                    print(f"Warning: Volume not found for paper {paperId}")

        print(f'Added {count} paper_publishedIn relations to {output} for journals')


def create_event_hasEdition_edge(output, paper_event_ed):
    edition_event_df = paper_event_ed[['venue_id', 'editionId']].drop_duplicates()
    edition_event_df.rename(columns={'venue_id': 'eventId'}, inplace=True)
    edition_event_df.to_csv(output, index=False)
    print(f'Added {len(edition_event_df)} event_hasEdition relations to {output} for events')

def create_journal_hasVolume_edge(output, vol_journal):
    edge_df = vol_journal[['journalId', 'volumeId']].drop_duplicates()
    edge_df.to_csv(output, index=False)
    print(f'Added {len(edge_df)} journal_hasVolume relations to {output}')

def create_paper_citedIn_edge(output, all_papers):
    count = 0

    with open(output, 'w', newline='') as csvfile:
        columns = [
                 "paperId", "citingPaperId"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for paper in all_papers:
            
            if paper is None:  # Skip None entries
                continue
            
            paperId=paper.get('paperId', None)
            citations = paper.get('citations', None)
            for citation in citations:
                citationId=citation.get('paperId', None)
                if citationId is not None:
                    writer.writerow({
                        'paperId': paperId,
                        'citingPaperId': citationId
                    })
                    count += 1

        print(f'Added {count} paper_citedIn relations to {output}')

def main(args: argparse.Namespace) -> None:

    all_papers=load_json(os.path.join(args.data_path, 'combined_papers_data.json'))
    paper_reviews=load_json(os.path.join(args.data_path, 'paper_reviewers.json'))
    paper_event_ed=pd.read_csv(os.path.join(args.output_path, 'ed_conf_merged.csv'))
    vol_journal=pd.read_csv(os.path.join(args.output_path, 'vol_journal_map.csv'))
    keyword_to_Id=load_keyword_to_id(os.path.join(args.output_path, 'keyword.csv'))
    volume_to_id=load_volume_to_id(os.path.join(args.output_path, 'vol_journal_map.csv'))
    paper_keywords = load_json(os.path.join(args.data_path,'paper_keywords.json'))
    create_paper_isRelatedTo_edge(os.path.join(args.output_path,'paper_isRelatedTo_keyword.csv'), paper_keywords , keyword_to_Id)
    create_author_wrote_edge(os.path.join(args.output_path,'author_wrote_paper.csv'), all_papers)
    create_paper_correspondedBy_edge(os.path.join(args.output_path,'paper_correspondedBy_author.csv'), all_papers)
    create_paper_publishedIn_vol_edge(os.path.join(args.output_path,'paper_publishedIn_volume.csv'), all_papers, volume_to_id)
    create_paper_publishedIn_ed_edge(os.path.join(args.output_path,'paper_publishedIn_edition.csv'), paper_event_ed)
    create_event_hasEdition_edge(os.path.join(args.output_path,'event_hasEdition_edition.csv'), paper_event_ed)
    create_journal_hasVolume_edge(os.path.join(args.output_path,'journal_hasVolume_volume.csv'), vol_journal)
    create_paper_citedIn_edge(os.path.join(args.output_path,'paper_citedIn_paper.csv'), all_papers)
    create_author_reviewed_edge(os.path.join(args.output_path, 'author_reviewed_paper.csv'), paper_reviews)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('data_path', default='/Users/elnararb/Documents/UPC/Semantic Data Management/data/')
    parser.add_argument('output_path', default='/Users/elnararb/Documents/UPC/Semantic Data Management/data/nodes_edges/')
    args = parser.parse_args()
    main(args)
