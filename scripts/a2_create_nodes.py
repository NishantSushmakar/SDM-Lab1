
import argparse
import csv
import os
import json
#from rake_nltk import Rake
import uuid
import pandas as pd

def clean_field(value):
    """Consistent field cleaning for CSV output"""
    value = str(value).strip()
    # Replace problematic characters
    value = value.replace('\r', ' ').replace('\n', ' ')  # Remove newlines
    value = value.replace('"', "'")  # Standardize to single quotes
    value = value.replace('\\', '/')  # Handle backslashes
    return value

def load_json(json_file_path):
    if os.path.exists(json_file_path):
        try:
            with open(json_file_path, 'r') as f:
                all_elements = json.load(f)
                print(f"Loaded {len(all_elements)} elements from {json_file_path}")
                return all_elements
        except json.JSONDecodeError:
            print(f"Error loading JSON from {json_file_path}, starting with empty list")


def create_paper_node(output,all_papers):
    count = 0

    with open(output, 'w', newline='') as csvfile: 
        # Extracting columns about paper
        columns = [
                "paperId", "url", "title", "abstract"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns, quoting=csv.QUOTE_ALL,
                          escapechar='\\')
        writer.writeheader()

        author_count=0 #for consistency check
        for paper in all_papers:
            cleaned_paper = {
                'paperId': clean_field(paper['paperId']),
                'url': clean_field(paper['url']),
                'title': clean_field(paper['title']),
                'abstract': clean_field(paper['abstract'])
            }

            if paper is None:  # Skip None entries
                continue
            writer.writerow(cleaned_paper)
            count += 1
            author_count+=len(paper['authors'])

        print(f'Added {count} papers to {output} with author count {author_count}')

def create_keywords_node(output,keywords_file):
    count = 0
    distinct_keywords = set()
    for paper_id, keywords in keywords_file.items():
        distinct_keywords.update(kw.strip().lower() for kw in keywords if kw.strip())

    with open(output, 'w', newline='') as csvfile: 
        # Extracting columns about paper
        columns = [
                "keywordId", "keyword"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for keyword in distinct_keywords:
            
            writer.writerow({
                'keywordId': str(uuid.uuid4()),
                'keyword': keyword
                })
            count += 1

        print(f'Added {count} keywords to {output} from total {len(distinct_keywords)}')

def create_journal_node(output, all_papers):
    count = 0
    unique_journal_ids = set()
    with open(output, 'w', newline='') as csvfile:
        # Extracting columns about paper
        columns = [
                "journalId", "name", "ISSN", "url"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for paper in all_papers:
            
            if paper is None:  # Skip None entries
                continue
            if ('JournalArticle' in paper.get('publicationTypes', None)
            and 'Conference' not in paper.get('publicationTypes', None) and paper.get('publicationVenue') is not None):
                venue = paper.get('publicationVenue')
                journal_id = venue.get('id')
                # Check if the journal ID is already processed
                if journal_id not in unique_journal_ids:
                    writer.writerow({
                        'journalId': journal_id,
                        'name': venue.get('name',None),
                        'ISSN': venue.get('issn', None),
                        'url': venue.get('url', None)
                    })
                    unique_journal_ids.add(journal_id)
                    count += 1

        print(f'Added {count} journals to {output}')

def create_volume_node(output, mapping, all_papers):
    count = 0
    unique_volumes = set()
    with open(output, 'w', newline='') as csvfile:
        # Extracting columns about paper
        columns = [
                "volumeId", "number", "year"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        journal_to_volume_mapping = {}
        for paper in all_papers:
            
            if paper is None:  # Skip None entries
                continue

            year=str(paper.get('year', None)).strip()
            venue=paper.get('publicationVenue')
            if ('JournalArticle' in paper.get('publicationTypes', None)
            and 'Conference' not in paper.get('publicationTypes', None) and venue is not None ):
                journal = paper.get('journal', None)
                number=str(journal.get('volume',None)).strip()
                journal_id = str(venue.get('id')).strip()
                volume_key = (journal_id, year, number)
                # print(f"Volume Key Created: {volume_key}")
                if volume_key not in unique_volumes:
                    volumeId=str(uuid.uuid4())
                    writer.writerow({
                        'volumeId': volumeId,
                        'number': number,
                        'year': year
                    })
                    journal_to_volume_mapping[volume_key] = volumeId
                    unique_volumes.add(volume_key)
                    count += 1

        print(f'Added {count} volumes to {output}')

    with open(mapping, 'w', newline='') as mapcsv:
        mapwriter = csv.writer(mapcsv)
        mapwriter.writerow(['journalId', 'year', 'number', 'volumeId'])  # Write header
        for (journal_id, year, number), volume_id in journal_to_volume_mapping.items():
            mapwriter.writerow([journal_id, year, number, volume_id])

def create_event_node(output,all_papers):
    count = 0
    unique_conf_ids = set()
    with open(output, 'w', newline='') as csvfile:
        # Extracting columns about paper
        columns = [
                "eventId", "name", "ISSN", "url", "type"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for paper in all_papers:
            
            if paper is None:  # Skip None entries
                continue
            if 'Conference' in paper.get('publicationTypes', None) and paper.get('publicationVenue') is not None :
                venue = paper.get('publicationVenue', None)
                
                conf_id = venue.get('id')
                conf_name = venue.get('name')
                if conf_id not in unique_conf_ids:

                    unique_conf_ids.add(conf_id)
                    writer.writerow({
                        'eventId': conf_id,
                        'name': conf_name,
                        'ISSN': venue.get('issn', None),
                        'url': venue.get('url', None),
                        'type': 'Workshop' if 'workshop' in conf_name.lower() else 'Conference'
                    })
                    count += 1

        print(f'Added {count} conferences to {output}')

def create_edition_node(output, merged, ed_info):
    distinct_editions = ed_info[['edition', 'location', 'year', 'venue_id']].drop_duplicates()
    distinct_editions['editionId'] = [str(uuid.uuid4()) for _ in range(len(distinct_editions))]
    ed_info = pd.merge(ed_info, distinct_editions, on=['edition', 'location', 'year','venue_id'], how='left')
    ed_info.to_csv(merged, index=False)
    distinct_editions.drop('venue_id', axis=1, inplace=True)
    distinct_editions = distinct_editions[['editionId', 'edition', 'location', 'year']]
    distinct_editions.to_csv(output, index=False)
    print(f'Added {len(distinct_editions)} editions to {output}')

def create_author_node(output,all_papers):
    count = 0
    unique_author_ids = set()

    with open(output, 'w', newline='') as csvfile: 
        columns = [
                "authorId", "name"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        for paper in all_papers:
            if paper is None:  # Skip None entries
                continue
            authors = paper.get('authors', None)
            for author in authors:
                author_id = author.get('authorId', None)
                # Check if the journal ID is already processed
                if author_id not in unique_author_ids and author_id is not None:
                    writer.writerow({
                        'authorId': author_id,
                        'name': author.get('name', None)
                    })
                    unique_author_ids.add(author_id)
                    count += 1

        print(f'Added {count} authors to {output}')

def main(args: argparse.Namespace) -> None:
    ed_data = pd.read_csv(os.path.join(args.data_path, 'paper_proceedings_location_new.csv'))
    all_papers=load_json(os.path.join(args.data_path, 'combined_papers_data.json'))
    keywords_file=load_json(os.path.join(args.data_path, 'paper_keywords.json'))
    create_paper_node(os.path.join(args.output_path, 'paper.csv'), all_papers)
    create_keywords_node(os.path.join(args.output_path, 'keyword.csv'), keywords_file)
    create_edition_node(os.path.join(args.output_path, 'edition.csv'), os.path.join(args.output_path, 'ed_conf_merged.csv'), ed_data)
    create_event_node(os.path.join(args.output_path,'event.csv'), all_papers)
    create_volume_node(os.path.join(args.output_path,'volume.csv'), os.path.join(args.output_path, 'vol_journal_map.csv'), all_papers)
    create_journal_node(os.path.join(args.output_path,'journal.csv'), all_papers)
    create_author_node(os.path.join(args.output_path,'author.csv'), all_papers)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('data_path', default='/Users/elnararb/Documents/UPC/Semantic Data Management/data/')
    parser.add_argument('output_path', default='/Users/elnararb/Documents/UPC/Semantic Data Management/data/nodes_edges/')
    args = parser.parse_args()
    main(args)
