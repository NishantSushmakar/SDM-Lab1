
import argparse
import csv
import os
import json
import uuid

def load_json(json_file_path):
    """Loading json files"""
    if os.path.exists(json_file_path):
        try:
            with open(json_file_path, 'r') as f:
                all_elements = json.load(f)
                print(f"Loaded {len(all_elements)} elements from {json_file_path}")
                return all_elements
        except json.JSONDecodeError:
            print(f"Error loading JSON from {json_file_path}, starting with empty list")

def create_reviews(output_edge, paper_reviews):
    count_edge=0
    with open(output_edge, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(['authorId','paperId','comments', 'vote'])
        
        for paper_id, reviewers in paper_reviews.items():
            for reviewer_key, data in reviewers.items():
                writer.writerow([
                    data['authorId'],
                    paper_id,
                    data['comments'],
                    data['vote']
                ])
                count_edge+=1
    print(f"Added {count_edge} to {output_edge}")

def create_affiliation(output_node, output_edge, author_details):

    
    unique_affiliations = set()
    author_affiliation_pairs = set() # Collecting distinct author-affiliation pairs for edge creation
    for author in author_details:
        if author is None:  # Skip None entries
                continue

        affiliations=author.get('affiliations', [])
        for affiliation in affiliations:
            if affiliation:
                unique_affiliations.add(str(affiliation).strip())  # Remove whitespace and add to set
                author_affiliation_pairs.add(
                    (author['authorId'], str(affiliation).strip()))

    affiliation_nodes = [{'affId': str(uuid.uuid4()), 'name': aff} for aff in unique_affiliations]
    affiliation_uuid_map = {aff['name']: aff['affId'] for aff in affiliation_nodes}

    missing_affiliations = set()
    for author_id, aff_name in author_affiliation_pairs:
        if aff_name not in affiliation_uuid_map:
            missing_affiliations.add(aff_name)

    if missing_affiliations:
        print("Missing affiliations in UUID map:", missing_affiliations)

    with open(output_node, 'w', newline='') as csvfile:
        columns = [
                "affId", "name"
            ]
        writer = csv.DictWriter(csvfile, fieldnames=columns, quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        writer.writerows(affiliation_nodes)
    print(f"Added {len(affiliation_nodes)} to {output_node}")

    with open(output_edge, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['authorId', 'affId'], quoting=csv.QUOTE_NONNUMERIC)
        writer.writeheader()
        for author_id, aff_name in author_affiliation_pairs:
            writer.writerow({
                'authorId': author_id,
                'affId': affiliation_uuid_map[aff_name]
            })
    print(f"Added {len(author_affiliation_pairs)} to {output_edge}")


def create_add_data(data_path,output_path) :
    author_details = load_json(os.path.join(data_path, 'authors_details_new.json'))
    paper_reviews = load_json(os.path.join(data_path, 'paper_reviewers_metadata.json'))
    
    create_affiliation(os.path.join(output_path,'affiliation.csv'), os.path.join(output_path, 'author_affiliatedWith_affiliation.csv'), author_details)
    create_reviews(os.path.join(output_path, 'review_relations.csv'), paper_reviews)
    
    print("Created Additional Data Successfully!!!")